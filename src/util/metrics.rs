use std::{env, sync::Arc};

use lazy_static::lazy_static;
use opentelemetry::{
    global,
    metrics::{Counter, Gauge, Meter},
    trace::TracerProvider,
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::Resource;
use reqwest_middleware::ClientWithMiddleware;
use reqwest_tracing::{SpanBackendWithUrl, TracingMiddleware};
use serenity::futures::future::Either;
use tracing_subscriber::{Layer, filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

lazy_static! {
    static ref _ROOT: Meter = global::meter("carrot_cake");
    pub static ref WS_CONNECT: Counter<u64> = _ROOT.u64_counter("bb.twitch.ws.connect").build();
    pub static ref WS_ERROR: Counter<u64> = _ROOT.u64_counter("bb.twitch.ws.error").build();
    pub static ref WS_RECEIVED: Counter<u64> =
        _ROOT.u64_counter("bb.twitch.ws.received_messages").build();
    pub static ref TWITCH_MSG_RECEIVED: Counter<u64> =
        _ROOT.u64_counter("bb.twitch.msg.received_messages").build();
    pub static ref TOKEN_REFRESH: Counter<u64> =
        _ROOT.u64_counter("bb.twitch.token.refresh").build();
    pub static ref TOKEN_TTL: Gauge<u64> = _ROOT
        .u64_gauge("bb.twitch.token.life")
        .with_unit("seconds")
        .build();
    pub static ref GUILDS: Gauge<u64> = _ROOT.u64_gauge("bb.discord.guilds").build();
    pub static ref UPDATES: Counter<u64> = _ROOT.u64_counter("bb.discord.updates").build();
    pub static ref MESSAGES: Counter<u64> = _ROOT.u64_counter("bb.discord.message").build();
}

pub async fn init() -> anyhow::Result<Box<dyn FnOnce()>> {
    if env::var("OTEL_ENABLED").is_ok() {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()?;
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(Resource::builder().with_service_name("carrot_cake").build())
            .with_batch_exporter(otlp_exporter)
            .build();

        let oltp_exporter_metrics = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .build()?;
        let metrics_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_resource(Resource::builder().with_service_name("carrot_cake").build())
            .with_periodic_exporter(oltp_exporter_metrics)
            .build();

        let oltp_exporter_logs = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .build()?;
        let logs_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_resource(Resource::builder().with_service_name("carrot_cake").build())
            .with_batch_exporter(oltp_exporter_logs)
            .build();
        let bridge = OpenTelemetryTracingBridge::new(&logs_provider);

        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(metrics_provider.clone());

        let tracer = tracer_provider.tracer("carrot_cake");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::Registry::default()
            .with(telemetry)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(EnvFilter::new("info,carrot_cake::twitch=trace")),
            )
            .with(bridge.with_filter(EnvFilter::new("info,carrot_cake::twitch=trace")))
            .try_init()?;

        Ok(Box::new(move || {
            if let Err(e) = tracer_provider.shutdown() {
                log::error!("error shutting down tracer_provider: {e}");
            }
            if let Err(e) = metrics_provider.shutdown() {
                log::error!("error shutting down metrics_provider: {e}");
            }
            if let Err(e) = logs_provider.shutdown() {
                log::error!("error shutting down logs_provider: {e}");
            }
        }))
    } else {
        tracing_subscriber::Registry::default()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_filter(EnvFilter::new("info,carrot_cake::twitch=trace")),
            )
            .try_init()?;

        Ok(Box::new(move || {
            // Do nothing
        }))
    }
}

#[derive(Clone, Debug)]
pub struct TracedHttpClient(Arc<ClientWithMiddleware>);

pub fn client() -> TracedHttpClient {
    let client = reqwest::Client::default();

    TracedHttpClient(Arc::new(
        reqwest_middleware::ClientBuilder::new(client)
            .with(TracingMiddleware::<SpanBackendWithUrl>::new())
            .build(),
    ))
}

impl twitch_api::HttpClient for TracedHttpClient {
    type Error = reqwest_middleware::Error;

    fn req(
        &self,
        request: twitch_api::client::Request,
    ) -> impl Future<Output = Result<twitch_api::client::Response, Self::Error>> + Send + use<>
    {
        // Reqwest plays really nice here and has a try_from on `http::Request` -> `reqwest::Request`
        use std::convert::TryFrom;
        let req = match reqwest::Request::try_from(request) {
            Ok(req) => req,
            Err(e) => return Either::Right(async { Err(reqwest_middleware::Error::Reqwest(e)) }),
        };
        let client = self.0.clone();
        let fut = async move {
            // Await the request and translate to `http::Response`
            let mut response = client.execute(req).await?;
            let mut result = http::Response::builder().status(response.status());
            let headers = result
                .headers_mut()
                // This should not fail, we just created the response.
                .expect("expected to get headers mut when building response");
            std::mem::swap(headers, response.headers_mut());
            let result = result.version(response.version());
            Ok(result
                .body(response.bytes().await?)
                .expect("mismatch reqwest -> http conversion should not fail"))
        };
        Either::Left(fut)
    }
}

macro_rules! increment {
    ($metric: expr$(; $($key:literal : $value:expr),*)?) => {
        {&$metric}.add(1, &[
            $($(opentelemetry::KeyValue::new($key, $value)),*)?
        ])

    };
}
pub(crate) use increment;

macro_rules! record {
    ($metric: expr, $v: expr$(; $($key:literal : $value:expr),*)?) => {
        {&$metric}.record($v, &[
            $($(opentelemetry::KeyValue::new($key, $value)),*)?
        ])

    };
}
pub(crate) use record;
