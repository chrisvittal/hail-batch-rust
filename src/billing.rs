#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Project {
    #[serde(rename = "billing_project")]
    pub name: String,
    pub users: Vec<String>,
}
