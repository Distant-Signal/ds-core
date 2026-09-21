//! Minimal RDM Train Operating Company List client, for the live tier
//! only (`reference::ReferenceData::fetch_live`).
//!
//! This deliberately duplicates a small slice of
//! `crates/poller-tocs/src/schema.rs`'s XML shape rather than depending on
//! that crate: `poller-tocs` has no `lib.rs` (it's a binary-only crate --
//! `main.rs` + private `mod schema`/`config`), so there is nothing to
//! import from it without restructuring that crate, which is out of scope
//! for this validator. The shape is small (RSPS5050 P-03-00 Rev A §3) and
//! unlikely to drift silently; if `poller-tocs` is ever split into a
//! lib+bin, this module should be deleted in favour of importing its real
//! `schema::parse_tocs` instead of re-deriving it here.
//!
//! Per `crates/poller-tocs/src/config.rs`'s own doc comment, the base URL
//! for this feed has **no confirmed value** in the current RSPS5050
//! edition -- there is no default here either, matching that crate's
//! choice to fail loudly rather than guess. See
//! `.github/workflows/validate-line-catalogue.yml` for exactly which two
//! secrets this needs before the live tier's operator check can run for
//! real.

use std::collections::HashMap;

use anyhow::Result;
use serde::Deserialize;

use common::ingest::RDM_AUTH_HEADER_NAME;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TrainOperatingCompanyList {
    #[serde(default, rename = "TrainOperatingCompany")]
    train_operating_company: Vec<TrainOperatingCompany>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TrainOperatingCompany {
    atoc_code: String,
    name: String,
}

pub async fn fetch_rdm_tocs(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
) -> Result<HashMap<String, String>> {
    let body = client
        .get(base_url)
        .header(RDM_AUTH_HEADER_NAME, api_key)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let list: TrainOperatingCompanyList = quick_xml::de::from_str(&body)?;
    Ok(list
        .train_operating_company
        .into_iter()
        .map(|toc| (toc.atoc_code.to_ascii_uppercase(), toc.name))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_spec_example_shape() {
        let xml = r#"
            <TrainOperatingCompanyList>
                <TrainOperatingCompany>
                    <AtocCode>LE</AtocCode>
                    <Name>Greater Anglia</Name>
                    <LegalName>London Eastern Railways</LegalName>
                </TrainOperatingCompany>
            </TrainOperatingCompanyList>
        "#;
        let list: TrainOperatingCompanyList = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(list.train_operating_company.len(), 1);
        assert_eq!(list.train_operating_company[0].atoc_code, "LE");
    }
}
