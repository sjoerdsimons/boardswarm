use std::sync::Arc;

use http::{Request, Response};
use serde::Deserialize;
use tower::{Layer, Service};
use tower_oauth2_resource_server::{
    auth_resolver::KidAuthorizerResolver, server::OAuth2ResourceServer, tenant::TenantConfiguration,
};

use crate::{config::Scalar, registry::Verifier};

#[derive(Clone, Debug, Deserialize, Default)]
pub struct Claims {
    #[serde(flatten)]
    values: serde_json::Value,
}

pub async fn setup_auth_layer(
    config: &[crate::config::Authentication],
) -> anyhow::Result<OAuth2ResourceServer<Claims>> {
    let mut resource =
        OAuth2ResourceServer::<Claims>::builder().auth_resolver(Arc::new(KidAuthorizerResolver {}));
    for auth in config {
        let tenant = match auth {
            crate::config::Authentication::Oidc { uri, audience, .. } => {
                TenantConfiguration::builder(uri)
                    .audiences(audience.as_slice())
                    .build()
                    .await?
            }
            crate::config::Authentication::Jwks { path, identifier } => {
                let jwks = tokio::fs::read_to_string(path).await?;
                let t = TenantConfiguration::static_builder(jwks);
                let t = if let Some(identifier) = identifier {
                    t.identifier(identifier)
                } else {
                    t
                };
                t.build()?
            }
        };
        resource = resource.add_tenant(tenant);
    }
    Ok(resource.build().await?)
}

#[derive(Clone, Debug, Default)]
pub struct Roles {
    pub roles: Arc<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct RoleVerifier {
    // When enforce is false, the allow if both the credentials and acl list are empty
    // else reject
    enforce: bool,
}

impl RoleVerifier {
    pub fn new(enforce: bool) -> Self {
        Self { enforce }
    }
}

impl Verifier for RoleVerifier {
    type Credential = Roles;
    type Acl = Vec<String>;
    fn verify(&self, cred: &Self::Credential, acl: &Self::Acl) -> bool {
        if self.enforce && cred.roles.is_empty() && acl.is_empty() {
            true
        } else {
            acl.iter().any(|role| cred.roles.iter().any(|r| r == role))
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoleLayer {
    roles: Arc<Vec<crate::config::Role>>,
}

impl RoleLayer {
    pub fn new(roles: Vec<crate::config::Role>) -> Self {
        Self {
            roles: Arc::new(roles),
        }
    }
}

impl<S> Layer<S> for RoleLayer {
    type Service = RoleService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RoleService {
            inner,
            roles: self.roles.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoleService<S> {
    inner: S,
    roles: Arc<Vec<crate::config::Role>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RoleService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone,
    S: Send + Sync + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        let extensions = req.extensions();
        if let Some(claims) = extensions.get::<Claims>() {
            let roles = claims_to_roles(claims, &self.roles);
            req.extensions_mut().insert(roles);
        }
        self.inner.call(req)
    }
}

impl PartialEq<Scalar> for serde_json::Value {
    fn eq(&self, other: &Scalar) -> bool {
        match (self, other) {
            (serde_json::Value::Bool(j), Scalar::Bool(s)) => j == s,
            (serde_json::Value::Number(j), Scalar::Number(s)) => {
                if let Some(j) = j.as_u64() {
                    if let Some(s) = s.as_u64() {
                        return j == s;
                    }
                }
                if let Some(j) = j.as_i64() {
                    if let Some(s) = s.as_i64() {
                        return j == s;
                    }
                }
                if let Some(j) = j.as_f64() {
                    if let Some(s) = s.as_f64() {
                        return j == s;
                    }
                }
                false
            }
            (serde_json::Value::String(j), Scalar::String(s)) => j == s,
            _ => false,
        }
    }
}

fn claim_matches_role(role: &crate::config::Role, claims: &Claims) -> bool {
    role.matches.iter().any(|m| {
        // TODO match identifier
        m.match_.iter().all(|(p, expected)| {
            let Some(value) = claims.values.pointer(p) else {
                return false;
            };
            if let serde_json::Value::Array(v) = value {
                v.iter().any(|value| value == expected)
            } else {
                value == expected
            }
        })
    })
}

fn claims_to_roles(claims: &Claims, roles: &[crate::config::Role]) -> Roles {
    let roles = roles
        .iter()
        .filter_map(|r| {
            if claim_matches_role(r, claims) {
                Some(r.role.clone())
            } else {
                None
            }
        })
        .collect();
    Roles {
        roles: Arc::new(roles),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_role() {
        let claims: Claims = serde_json::from_str(r#"{ "user": "boardswarm" }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/user": "boardswarm"
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["test"])
    }

    #[test]
    fn role_u64_num() {
        let claims: Claims = serde_json::from_str(r#"{ "badgers": 128 }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/badgers": 128
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["test"])
    }

    #[test]
    fn role_i64_num() {
        let claims: Claims = serde_json::from_str(r#"{ "badgers": -128 }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/badgers": -128
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["test"])
    }

    #[test]
    fn role_f64_num() {
        let claims: Claims = serde_json::from_str(r#"{ "badgers": -128.3 }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/badgers": -128.3
        "#,
        )
        .unwrap();
        eprintln!("{:?}", config);
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["test"])
    }

    #[test]
    fn role_num_notmatching() {
        let claims: Claims = serde_json::from_str(r#"{ "badgers": 128 }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/badgers": -128.3
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert!(roles.roles.is_empty())
    }

    #[test]
    fn role_bool() {
        let claims: Claims = serde_json::from_str(r#"{ "admin": true }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/admin": true
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(*roles.roles, &["test"])
    }

    #[test]
    fn role_bool_notmatching() {
        let claims: Claims = serde_json::from_str(r#"{ "admin": true }"#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: test
            matches:
              - identifier: auth
                match:
                  "/admin": false
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert!(roles.roles.is_empty());
    }

    #[test]
    fn role_one_in_array() {
        let claims: Claims = serde_json::from_str(r#" { "groups": [ "a", "b" ] } "#).unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: match-a-group
            matches:
              - identifier: auth
                match:
                  "/groups": "a"
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["match-a-group"])
    }

    #[test]
    fn multiple_roles() {
        let claims: Claims = serde_json::from_str(
            r#"
        {
          "user": "boardswarm",
          "groups": [ "a", "b" ]
        }
            "#,
        )
        .unwrap();
        let config: Vec<crate::config::Role> = serde_yaml::from_str(
            r#"
          - role: match-user
            matches:
              - identifier: auth
                match:
                  "/user": "boardswarm"
          - role: match-a-group
            matches:
              - identifier: auth
                match:
                  "/groups": "a"
          - role: nomatch
            matches:
              - identifier: auth
                match:
                  "/groups": "nope"
        "#,
        )
        .unwrap();
        let roles = claims_to_roles(&claims, &config);
        assert_eq!(roles.roles.as_slice(), &["match-user", "match-a-group"])
    }
}
