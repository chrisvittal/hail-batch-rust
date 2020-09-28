use crate::{util, Client, Error};
use serde::{
    de::{Error as DeError, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::stream::StreamExt;

impl Client {
    pub fn new_batch(&self) -> crate::Result<BatchBuilder> {
        if self.data.billing_project.is_none() {
            Err(Error::Msg(
                "cannot build batch without a billing project".into(),
            ))
        } else {
            Ok(BatchBuilder {
                bc: self.clone(),
                attributes: HashMap::new(),
                callback: None,
                jobs: Vec::new(),
            })
        }
    }
}

#[derive(Debug)]
pub struct BatchBuilder {
    bc: Client,
    attributes: HashMap<String, String>,
    callback: Option<reqwest::Url>,
    jobs: Vec<JobSpec>,
}

#[derive(Debug)]
pub struct Batch {
    bc: Client,
    id: u64,
    attributes: HashMap<String, String>,
    jobs: Vec<JobSpec>,
}

#[derive(Debug, Serialize)]
struct BatchSpec<'a> {
    attributes: &'a HashMap<String, String>,
    billing_project: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback: Option<&'a reqwest::Url>,
    n_jobs: usize,
    token: String,
}

impl BatchBuilder {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.attribute("name", name.into())
    }

    pub fn attribute(&mut self, key: impl ToString, value: impl ToString) -> &mut Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    pub fn attributes<I, S, T>(&mut self, attrs: I) -> &mut Self
    where
        S: ToString,
        T: ToString,
        I: IntoIterator<Item = (S, T)>,
    {
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()));
        self.attributes.extend(attrs);
        self
    }

    pub fn callback(&mut self, url: impl reqwest::IntoUrl) -> crate::Result<&mut Self> {
        self.callback.replace(url.into_url()?);
        Ok(self)
    }

    /// Get a builder to create a new job.
    pub fn new_job(&mut self, image: impl Into<String>, cmd: impl Into<String>) -> JobBuilder<'_> {
        JobBuilder::new(self, image.into(), cmd.into())
    }

    /// Add a job to this batch from a spec, usually created from deserialization, returns the
    /// builder.
    ///
    /// This will error if the job has invalid parents, that is, if the job's id is less than any
    /// parent ids.
    pub fn add_job(&mut self, mut spec: JobSpec) -> crate::Result<JobBuilder<'_>> {
        spec.id = self.jobs.len() + 1;
        for &id in &spec.parent_ids {
            if id >= spec.id {
                return Err(Error::Msg(
                    format!(
                        "invalid parent id {}, parent ids must be less than job ids (which was {}",
                        id, spec.id
                    )
                    .into(),
                ));
            }
        }
        Ok(JobBuilder { bb: self, spec })
    }

    async fn submit_jobs(&self, id: u64, specses: Vec<Vec<Vec<u8>>>) -> crate::Result<()> {
        use tokio::sync::mpsc::{self, error::TryRecvError};
        if specses.is_empty() {
            return Ok(());
        }

        let endpoint = Arc::new(format!("/api/v1alpha/batches/{}/jobs/create", id));

        let (tx, mut rx) = mpsc::channel(10);
        let n_reqs = specses.len();
        let mut reqs = 0;
        for specs in specses {
            match rx.try_recv() {
                Ok(Ok(_)) => reqs += 1,
                Ok(Err(e)) => return Err(e),
                Err(TryRecvError::Closed) => {
                    panic!("submit_jobs: all senders have been dropped, this is a bug")
                }
                Err(TryRecvError::Empty) => {} // do nothing, spawn the next job
            }

            if let Ok(Err(e)) = rx.try_recv() {
                return Err(e);
            }

            let mut bytes = Vec::new();
            bytes.push(b'[');
            for spec in specs {
                bytes.extend_from_slice(&spec);
                bytes.push(b',');
            }
            if let Some(b) = bytes.last_mut() {
                *b = b']';
            }

            debug_assert!(
                serde_json::from_slice::<serde_json::Value>(&bytes).is_ok(),
                "bytes are not valid json"
            );

            let bc = self.bc.clone();
            let ep = endpoint.clone();
            let mut tx = tx.clone();
            tokio::spawn(async move {
                let resp = bc.post(&*ep, "application/json", bytes).await;
                let _ = tx.send(resp).await;
            });
        }

        std::mem::drop(tx);
        while let Some(resp) = rx.next().await {
            if let Err(e) = resp {
                return Err(e);
            }
            reqs += 1;
        }
        assert_eq!(reqs, n_reqs, "did not recieve enough responses");

        Ok(())
    }

    pub async fn submit(self) -> crate::Result<Batch> {
        #[derive(Deserialize)]
        struct BatchID {
            id: u64,
        }

        let spec = BatchSpec {
            attributes: &self.attributes,
            billing_project: self.bc.data.billing_project.as_deref().unwrap(),
            callback: self.callback.as_ref(),
            n_jobs: self.jobs.len(),
            token: util::gen_token(),
        };
        let BatchID { id } = self
            .bc
            .post_json("/api/v1alpha/batches/create", &spec)
            .await?
            .json()
            .await?;

        const MAX_BUNCH_SIZE: usize = 1024 * 1024;
        const MAX_BUNCH_JOBS: usize = 1024;

        let serialized_jobs = self.jobs.iter().map(|spec| {
            (
                spec.id,
                serde_json::to_vec(spec).expect("to_vec should not fail"),
            )
        });

        let mut bunches = Vec::new();
        let mut bunch = Vec::new();
        let mut bunch_bytes = 0;
        for (id, spec) in serialized_jobs {
            if spec.len() > MAX_BUNCH_SIZE {
                return Err(Error::Msg(
                    format!(
                        "job {} too large, was {}B which is greater than the limit of {}B",
                        id,
                        spec.len(),
                        MAX_BUNCH_SIZE
                    )
                    .into(),
                ));
            } else if bunch_bytes + spec.len() < MAX_BUNCH_SIZE && bunch.len() < MAX_BUNCH_JOBS {
                bunch_bytes += spec.len() + 1;
                bunch.push(spec);
            } else {
                bunches.push(bunch);
                bunch_bytes = spec.len() + 1;
                bunch = vec![spec];
            }
        }

        if !bunch.is_empty() {
            bunches.push(bunch);
        }

        self.submit_jobs(id, bunches).await?;

        let path = format!("/api/v1alpha/batches/{}/close", id);
        self.bc.patch(&path).await?;
        Ok(Batch {
            bc: self.bc,
            id,
            attributes: self.attributes,
            jobs: self.jobs,
        })
    }
}

impl Batch {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn web_url(&self) -> reqwest::Url {
        self.bc.join_url(&format!("/batches/{}", self.id))
    }

    pub async fn cancel(&self) -> crate::Result<()> {
        let ep = format!("/api/v1alpha/batches/{}/cancel", self.id);
        self.bc.patch(&ep).await.map(|_| ())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobSpec {
    #[serde(rename = "job_id", default)]
    id: usize,
    #[serde(skip_serializing_if = "<&bool as std::ops::Not>::not", default)]
    always_run: bool,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    attributes: HashMap<String, String>,
    command: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", with = "env_map", default)]
    env: HashMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    gcsfuse: Vec<GcsFuseMount>,
    image: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    input_files: Vec<FileMapping>,
    #[serde(default)]
    mount_docker_socket: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    output_files: Vec<FileMapping>,
    #[serde(default)]
    parent_ids: Vec<usize>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    requester_pays_project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    network: Option<String>,
    #[serde(default)]
    resources: Resources,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    secrets: Vec<Secret>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    service_account: Option<ServiceAccount>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    /// timout in seconds, None for no timeout,
    timeout: Option<NonZeroU64>,
}

impl JobSpec {
    pub fn id(&self) -> usize {
        self.id
    }
}

#[derive(Debug)]
pub struct JobBuilder<'bb> {
    bb: &'bb mut BatchBuilder,
    spec: JobSpec,
}

impl Deref for JobBuilder<'_> {
    type Target = JobSpec;
    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for JobBuilder<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsFuseMount {
    pub bucket: String,
    pub mount_path: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMapping {
    pub from: String,
    pub to: String,
}

impl FileMapping {
    fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceAccount {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Secret {
    pub namespace: String,
    pub name: String,
    pub mount_path: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Resources {
    #[serde(deserialize_with = "deserialize_cpu")]
    pub cpu: f64,
    #[serde(deserialize_with = "deserialize_mem")]
    pub memory: u64,
    #[serde(deserialize_with = "deserialize_mem")]
    pub storage: u64,
}

static MEM_RE_STR: &str = r"[+]?((?:[0-9]*[.])?[0-9]+)([KMGTP][i]?)?";
static CPU_RE_STR: &str = r"[+]?((?:[0-9]*[.])?[0-9]+)([m])?";
lazy_static::lazy_static! {
    static ref MEM_RE: regex::Regex = regex::Regex::new(MEM_RE_STR).unwrap();
    static ref CPU_RE: regex::Regex = regex::Regex::new(CPU_RE_STR).unwrap();
}

struct ReVisitor(&'static regex::Regex);

// FIXME, add numeric types in deserializing
impl<'de> Visitor<'de> for ReVisitor {
    type Value = (f64, Option<Cow<'de, str>>);

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "value to match regex, {:?}", self.0.as_str())
    }

    fn visit_str<E: DeError>(self, val: &str) -> Result<Self::Value, E> {
        if let Some(groups) = self.0.captures(val) {
            // these unwraps won't panic as we know we have a capture group, and we know
            // we have a floating point string.
            let v = groups.get(1).unwrap().as_str().parse().unwrap();
            Ok((v, groups.get(2).map(|m| Cow::from(m.as_str().to_owned()))))
        } else {
            Err(E::invalid_value(serde::de::Unexpected::Str(val), &self))
        }
    }

    fn visit_borrowed_str<E: DeError>(self, val: &'de str) -> Result<Self::Value, E> {
        if let Some(groups) = self.0.captures(val) {
            // these unwraps won't panic as we know we have a capture group, and we know
            // we have a floating point string.
            let v = groups.get(1).unwrap().as_str().parse().unwrap();
            Ok((v, groups.get(2).map(|m| m.as_str().into())))
        } else {
            Err(E::invalid_value(serde::de::Unexpected::Str(val), &self))
        }
    }
}

fn deserialize_mem<'de, D>(de: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let (val, suf) = de.deserialize_str(ReVisitor(&*MEM_RE))?;
    let mul = match suf.unwrap_or_else(|| "".into()).as_ref() {
        "K" => 1000f64,
        "Ki" => 1024f64,
        "M" => 1000f64.powi(2),
        "Mi" => 1024f64.powi(2),
        "G" => 1000f64.powi(3),
        "Gi" => 1024f64.powi(3),
        "T" => 1000f64.powi(4),
        "Ti" => 1024f64.powi(4),
        "P" => 1000f64.powi(5),
        "Pi" => 1024f64.powi(5),
        "" => 1.,
        _ => unreachable!(),
    };

    Ok((val * mul).ceil() as u64)
}

fn deserialize_cpu<'de, D>(de: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let (val, suf) = de.deserialize_str(ReVisitor(&*CPU_RE))?;
    if Some("m") == suf.as_deref() {
        Ok(val / 1000.)
    } else {
        Ok(val)
    }
}

// TODO: UGH WHY!!!!
impl Serialize for Resources {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Resources", 3)?;
        s.serialize_field("cpu", &self.cpu.to_string())?;
        s.serialize_field("memory", &self.memory.to_string())?;
        s.serialize_field("storage", &self.storage.to_string())?;
        s.end()
    }
}

impl Resources {
    const DEFAULT: Self = Resources {
        cpu: 1.,
        memory: (375 * 1024 * 1024 * 1024) / 100, // 3.75 GiB (in bytes)
        storage: 10 * 1024 * 1024 * 1024,         // 10 GiB (in bytes)
    };
}

impl Default for Resources {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl<'bb> JobBuilder<'bb> {
    fn new(bb: &'bb mut BatchBuilder, image: String, cmd: String) -> Self {
        let id = bb.jobs.len() + 1;
        Self {
            bb,
            spec: JobSpec {
                id,
                image,
                command: vec![cmd],
                always_run: false,
                attributes: HashMap::new(),
                env: HashMap::new(),
                gcsfuse: Vec::new(),
                input_files: Vec::new(),
                mount_docker_socket: false,
                output_files: Vec::new(),
                parent_ids: Vec::new(),
                port: None,
                requester_pays_project: None,
                network: None,
                resources: Resources::DEFAULT,
                secrets: Vec::new(),
                service_account: None,
                timeout: None,
            },
        }
    }
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.attribute("name", name.into())
    }

    pub fn attribute(&mut self, key: impl ToString, value: impl ToString) -> &mut Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    pub fn attributes<I, S, T>(&mut self, attrs: I) -> &mut Self
    where
        S: ToString,
        T: ToString,
        I: IntoIterator<Item = (S, T)>,
    {
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()));
        self.attributes.extend(attrs);
        self
    }

    /// Sets the `always_run` field of the job (Default: `false`).
    pub fn always_run(&mut self, always_run: bool) -> &mut Self {
        self.always_run = always_run;
        self
    }

    /// Adds a single argument to the command.
    pub fn arg(&mut self, arg: impl Into<String>) -> &mut Self {
        self.command.push(arg.into());
        self
    }

    /// Adds several arguments to the command.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = S>,
    {
        self.command.extend(args.into_iter().map(S::into));
        self
    }

    /// Returns the current arguments, including the command, for arbitrary manipulation
    pub fn args_mut(&mut self) -> &mut Vec<String> {
        &mut self.command
    }

    /// Add/Replace one environment variable.
    pub fn env(&mut self, key: impl Into<String>, val: impl Into<String>) -> &mut Self {
        self.env.insert(key.into(), val.into());
        self
    }

    /// Adds/Replace several environment variables.
    pub fn env_vars<I, S, T>(&mut self, vars: I) -> &mut Self
    where
        S: Into<String>,
        T: Into<String>,
        I: IntoIterator<Item = (S, T)>,
    {
        self.env
            .extend(vars.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Removes a key from the environment, if it exists
    pub fn env_remove(&mut self, key: &impl std::borrow::Borrow<str>) -> Option<String> {
        self.env.remove(key.borrow())
    }

    /// Clears the provided environment, doesn't affect the environment otherwise present
    /// in the image.
    pub fn env_clear(&mut self) {
        self.env.clear();
    }

    /// Adds a gcsfuse mount point.
    pub fn gcsfuse(
        &mut self,
        bucket: impl Into<String>,
        mount_path: impl Into<String>,
        read_only: bool,
    ) -> &mut Self {
        let gcsfuse_mount = GcsFuseMount {
            bucket: bucket.into(),
            mount_path: mount_path.into(),
            read_only,
        };
        self.gcsfuse.push(gcsfuse_mount);
        self
    }

    /// Adds an input file.
    pub fn input_file(&mut self, from: impl Into<String>, to: impl Into<String>) -> &mut Self {
        self.input_files.push(FileMapping::new(from, to));
        self
    }

    /// Adds several input files.
    pub fn input_files<I, S, T>(&mut self, paths: I) -> &mut Self
    where
        S: Into<String>,
        T: Into<String>,
        I: IntoIterator<Item = (S, T)>,
    {
        self.input_files
            .extend(paths.into_iter().map(|(f, t)| FileMapping::new(f, t)));
        self
    }

    /// Sets the `mount_docker_socket` property (Default: `false`).
    pub fn mount_docker_socket(&mut self, mount: bool) -> &mut Self {
        self.mount_docker_socket = mount;
        self
    }

    /// Adds an output file.
    pub fn output_file(&mut self, from: impl Into<String>, to: impl Into<String>) -> &mut Self {
        self.output_files.push(FileMapping::new(from, to));
        self
    }

    /// Adds several output files.
    pub fn output_files<I, S, T>(&mut self, paths: I) -> &mut Self
    where
        S: Into<String>,
        T: Into<String>,
        I: IntoIterator<Item = (S, T)>,
    {
        self.output_files
            .extend(paths.into_iter().map(|(f, t)| FileMapping::new(f, t)));
        self
    }

    /// Adds a parent job id. Job parents need to be handled carefully, once set, they cannot
    /// be unset. They are not deserialized when fetching job specs, and so have to be
    /// reconstructed using other means.
    ///
    /// # Panics
    /// Panics if parent_id is greater than or equal to this job's id
    pub fn parent(&mut self, parent_id: usize) -> &mut Self {
        assert!(parent_id < self.id, "invalid parent_id: {}", parent_id);
        self.parent_ids.push(parent_id);
        self
    }

    /// Adds several parent job ids. Job parents need to be handled carefully, once set, they
    /// cannot be unset. They are not deserialized when fetching job specs, and so have to be
    /// reconstructed using other means.
    ///
    /// # Panics
    /// Panics if any of the parent_ids are greater than or equal to this job's id
    pub fn parents(&mut self, parent_ids: impl IntoIterator<Item = usize>) -> &mut Self {
        let start = self.parent_ids.len();
        self.parent_ids.extend(parent_ids);
        if self.parent_ids[start..].iter().any(|&id| id >= self.id) {
            let invalids = self.parent_ids[start..]
                .iter()
                .filter(|&&id| id >= self.id)
                .collect::<Vec<_>>();
            panic!("invalid parent ids: {:?}", invalids);
        }
        self
    }

    /// Sets a port this container will publish (Default: no port will be published)
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port.replace(port);
        self
    }

    /// Clears published port (if any)
    pub fn clear_port(&mut self) -> &mut Self {
        self.port.take();
        self
    }

    /// Set a requester pays project for this job (Default: no requester pays project)
    pub fn requester_pays_project(&mut self, project: impl Into<String>) -> &mut Self {
        self.requester_pays_project.replace(project.into());
        self
    }

    /// Sets the network to use for this job. (Default: no special networking).
    pub fn network(&mut self, network: impl Into<String>) -> &mut Self {
        self.network.replace(network.into());
        self
    }

    /// Sets the requested resources for this job, `cpu` is the fraction of whole cpus (1.0 is
    /// 1 cpu, 0.5 is half a cpu, etc.), `memory` and `storage` are both expressed in bytes.
    /// (Defaults: 1 cpu, 3.75 GiB RAM, 10 GB Storage Space).
    pub fn resources(&mut self, cpu: f64, memory: u64, storage: u64) -> &mut Self {
        self.resources = Resources {
            cpu,
            memory,
            storage,
        };
        self
    }

    /// Sets the requested cpu. 1.0 is 1 whole cpu. (Default: 1 cpu)
    pub fn cpu(&mut self, cpu: f64) -> &mut Self {
        self.resources.cpu = cpu;
        self
    }

    /// Sets the requested memory in bytes. (Default: 3.75 GiB)
    pub fn memory(&mut self, memory: u64) -> &mut Self {
        self.resources.memory = memory;
        self
    }

    /// Sets the requested disk space in bytes. (Default: 10 GiB)
    pub fn storage(&mut self, storage: u64) -> &mut Self {
        self.resources.storage = storage;
        self
    }

    /// Sets the service account to use for this job.
    pub fn service_account(
        &mut self,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> &mut Self {
        self.service_account.replace(ServiceAccount {
            namespace: namespace.into(),
            name: name.into(),
        });
        self
    }

    /// Sets the timeout in seconds, setting it to zero clears the timeout.
    pub fn timeout(&mut self, timeout: u64) -> &mut Self {
        self.timeout = NonZeroU64::new(timeout);
        self
    }

    /// Finalizes this job, adding it to the batch and returning
    pub fn build(self) -> usize {
        let JobBuilder { spec, bb } = self;
        let id = spec.id;
        bb.jobs.push(spec);
        assert_eq!(id, bb.jobs.len(), "mismatch in job count and job id");
        id
    }
}

mod env_map {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct EnvMapping<'a> {
        name: Cow<'a, str>,
        value: Cow<'a, str>,
    }

    impl<'a> From<(&'a str, &'a str)> for EnvMapping<'a> {
        fn from((name, value): (&'a str, &'a str)) -> Self {
            Self {
                name: name.into(),
                value: value.into(),
            }
        }
    }

    impl<'a, S1: AsRef<str>, S2: AsRef<str>> From<(&'a S1, &'a S2)> for EnvMapping<'a> {
        fn from((name, value): (&'a S1, &'a S2)) -> Self {
            Self {
                name: name.as_ref().into(),
                value: value.as_ref().into(),
            }
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<HashMap<String, String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::SeqAccess;
        use std::fmt;
        struct EnvMapVisitor;

        impl<'de> Visitor<'de> for EnvMapVisitor {
            type Value = HashMap<String, String>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a sequence of name/value pairs")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut map = seq
                    .size_hint()
                    .map_or_else(HashMap::new, HashMap::with_capacity);
                while let Some(EnvMapping { name, value }) = seq.next_element()? {
                    map.insert(name.into(), value.into());
                }
                Ok(map)
            }
        }

        de.deserialize_seq(EnvMapVisitor)
    }

    pub fn serialize<S>(env: &HashMap<String, String>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;
        let len = env.len();
        let mut seq = ser.serialize_seq(Some(len))?;
        for map in env.iter().map(EnvMapping::from) {
            seq.serialize_element(&map)?;
        }
        seq.end()
    }
}
