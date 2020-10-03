use argh::FromArgs;
use color_eyre::eyre::{self, WrapErr};
use directories::BaseDirs;
use hail_batch::{batch, Client};
use serde::Deserialize;
use tokio::prelude::*;

#[derive(Clone, Copy, Debug)]
enum Format {
    Json,
    Yaml,
}

impl std::str::FromStr for Format {
    type Err = eyre::Report;
    fn from_str(s: &str) -> eyre::Result<Self> {
        match s {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            _ => eyre::bail!("unsupported output format: {}", s),
        }
    }
}

impl Format {
    fn get_vec_serializer<T: serde::Serialize + ?Sized>(
        &self,
    ) -> impl Fn(&T) -> eyre::Result<Vec<u8>> + '_ {
        move |item| {
            let mut v = match self {
                Format::Json => {
                    serde_json::to_vec_pretty(item).wrap_err("could not JSON serialize item")?
                }
                Format::Yaml => {
                    serde_yaml::to_vec(item).wrap_err("could not YAML serialize item")?
                }
            };
            v.push(b'\n');
            Ok(v)
        }
    }
}

#[derive(Debug, FromArgs)]
/// A rust client for the hail batch service,
/// https://batch.hail.is
struct Opts {
    #[argh(subcommand)]
    cmd: Command,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
enum Command {
    Billing(Billing),
    Submit(SubmitBatch),
}

#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "billing")]
/// Get information about billing projects
struct Billing {
    /// format to print the output, can be either 'json' or 'yaml' (the default).
    #[argh(option, short = 'o', default = "Format::Yaml")]
    format: Format,
    #[argh(subcommand)]
    cmd: BillingCmd,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand)]
enum BillingCmd {
    List(BillingList),
    Get(BillingGet),
}

#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "list")]
/// list billing projects
struct BillingList {}

/// query a specific billing project
#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "get")]
struct BillingGet {
    #[argh(positional)]
    /// name of the billing project to query
    name: String,
}

#[derive(Debug, FromArgs)]
#[argh(subcommand, name = "submit")]
/// Submit a batch from a yaml specification
struct SubmitBatch {
    #[argh(positional)]
    /// yaml to submit, standard input if not present
    path: Option<String>,
}

async fn get_token() -> eyre::Result<String> {
    use serde_json::{from_slice, Map, Value};

    let token_file = BaseDirs::new()
        .ok_or_else(|| eyre::eyre!("cannot find home dir"))?
        .home_dir()
        .join(".hail")
        .join("tokens.json");

    from_slice::<Map<String, Value>>(
        &tokio::fs::read(token_file)
            .await
            .wrap_err("error reading token file")?,
    )?
    .get("default")
    .ok_or_else(|| eyre::eyre!("'default' token not found, perhaps you need to login?"))?
    .as_str()
    .ok_or_else(|| eyre::eyre!("token is not a string"))
    .map(String::from)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let args: Opts = argh::from_env();
    let token = get_token().await?;
    match args.cmd {
        Command::Billing(Billing { cmd, format }) => {
            let client = Client::builder(token).build()?;
            let prj = match cmd {
                BillingCmd::List(_) => {
                    format.get_vec_serializer()(&client.list_billing_projects().await?)?
                }
                BillingCmd::Get(BillingGet { name }) => {
                    format.get_vec_serializer()(&client.get_billing_project(name).await?)?
                }
            };
            io::stdout()
                .write_all(&prj)
                .await
                .wrap_err("could not print output")
        }
        Command::Submit(SubmitBatch { path }) => {
            let BatchSpec {
                billing_project: bp,
                name,
                jobs,
            } = BatchSpec::read(path).await?;
            let client = Client::builder(token).billing_project(bp).build()?;
            let mut batch = client.new_batch()?;
            batch.name(name);
            for JobSpec { name, spec } in jobs {
                let mut jb = batch.add_job(spec)?;
                if let Some(name) = name {
                    jb.name(name);
                }
                jb.build();
            }
            let batch = batch.submit().await?;
            let msg = format!("Submitted batch {}, see {}\n", batch.id(), batch.web_url());
            io::stdout()
                .write_all(msg.as_ref())
                .await
                .wrap_err("could not print output")
        }
    }
}

#[derive(Debug, Deserialize)]
struct BatchSpec {
    name: String,
    billing_project: String,
    #[serde(default)]
    jobs: Vec<JobSpec>,
}

#[derive(Debug, Deserialize)]
struct JobSpec {
    #[serde(default)]
    name: Option<String>,
    #[serde(flatten)]
    spec: batch::JobSpec,
}

impl BatchSpec {
    async fn read(path: Option<String>) -> eyre::Result<Self> {
        let yaml_s = match path {
            Some(p) => tokio::fs::read(&p)
                .await
                .wrap_err(format!("error reading {}", p))?,
            None => {
                let mut v = Vec::new();
                tokio::io::stdin()
                    .read_to_end(&mut v)
                    .await
                    .map(|_| v)
                    .wrap_err("error reading standard input")?
            }
        };

        serde_yaml::from_slice(&yaml_s).wrap_err("invalid specification")
    }
}
