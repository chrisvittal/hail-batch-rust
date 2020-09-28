use crate::Error;
use reqwest::Response;

/// processes a response, returning it if it is OK,
/// decomposes it into an error if it's not ok.
pub async fn handle(resp: Response) -> crate::Result<Response> {
    if let Err(request_error) = resp.error_for_status_ref() {
        let extra = resp.text().await?;
        Err(Error::Service {
            extra,
            request_error,
        })
    } else {
        Ok(resp)
    }
}

pub fn gen_token() -> String {
    let tok: [u8; 32] = rand::random();
    base64::encode_config(tok, base64::URL_SAFE)
}
