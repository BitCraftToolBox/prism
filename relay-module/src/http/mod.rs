use spacetimedb::Table;
use spacetimedb::http::{Body, HandlerContext, Request, Response, Router};

use crate::tables::players::player_state;

#[spacetimedb::http::handler]
fn players(ctx: &mut HandlerContext, request: Request) -> Response {
    let url = url::Url::parse(request.uri().to_string().as_str());
    let query = url.ok().and_then(|u| {
        u.query_pairs()
            .find(|(k, _)| k == "q")
            .map(|(_, v)| v.into_owned())
    });

    let body = if let Some(q) = query {
        let q_lower = q.to_lowercase();
        let mut rows: Vec<_> = ctx.with_tx(|tx| {
            tx.db
                .player_state()
                .iter()
                .filter(|p| p.name.to_lowercase().contains(&q_lower))
                .collect()
        });

        rows.sort_by(|a, b| {
            let a_lower = a.name.to_lowercase();
            let b_lower = b.name.to_lowercase();

            let a_exact = a_lower == q_lower;
            let b_exact = b_lower == q_lower;

            if a_exact != b_exact {
                return if a_exact {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            }

            let a_starts = a_lower.starts_with(&q_lower);
            let b_starts = b_lower.starts_with(&q_lower);

            if a_starts != b_starts {
                return if a_starts {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                };
            }

            std::cmp::Ordering::Equal
        });

        let results: Vec<_> = rows
            .into_iter()
            .map(|p| {
                serde_json::json!({
                    "entityId": p.entity_id.to_string(),
                    "username": p.name,
                    "signedIn": p.online
                })
            })
            .collect();

        serde_json::to_vec(&results).ok()
    } else {
        Some(vec![])
    };

    if let Some(body) = body {
        Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(Body::from_bytes(body))
            .unwrap()
    } else {
        Response::builder().status(404).body(Body::empty()).unwrap()
    }
}

#[spacetimedb::http::router]
fn router() -> Router {
    Router::new().get("/players", players)
}
