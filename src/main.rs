use anyhow::{Context, anyhow};
use clap::Parser;
use futures::stream::StreamExt;
use litep2p::{
    Litep2p, Litep2pEvent, PeerId,
    config::ConfigBuilder,
    error::{DialError, NegotiationError},
    protocol::libp2p::bitswap::{
        BitswapEvent, BlockPresenceType, Config as BitswapConfig, ResponseType, WantType,
    },
    transport::{tcp::config::Config as TcpConfig, websocket::config::Config as WsConfig},
    types::{
        cid::Cid,
        multiaddr::{Multiaddr, Protocol},
    },
};
use log::{info, warn};
use std::{
    pin::pin,
    time::{Duration, Instant},
};
use tokio::time::sleep;

/// Operation timeout
const TIMEOUT: Duration = Duration::from_secs(10);

/// Tool to benchmark a Bitswap server
#[derive(Debug, Parser)]
pub struct Args {
    /// Address of the Bitswap server
    #[arg(short, long)]
    address: Multiaddr,

    /// CID to download repetitively
    #[arg(short, long)]
    cid: Cid,

    /// Number of iterations
    #[arg(short, long)]
    iterations: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let Args {
        address,
        cid,
        iterations,
    } = Args::parse();

    let (bitswap_config, mut bitswap) = BitswapConfig::new();

    let mut litep2p = Litep2p::new(
        ConfigBuilder::new()
            .with_tcp(TcpConfig {
                listen_addresses: Vec::new(),
                ..Default::default()
            })
            .with_websocket(WsConfig {
                listen_addresses: Vec::new(),
                ..Default::default()
            })
            .with_libp2p_bitswap(bitswap_config)
            .build(),
    )
    .context("litep2p initialization failed")?;

    let (mut dial_address, mut peer_id) =
        if let Some(Protocol::P2p(multihash)) = address.iter().last() {
            (
                address.clone(),
                Some(
                    PeerId::from_multihash(multihash)
                        .map_err(|_| anyhow!("p2p component is not peer ID"))?,
                ),
            )
        } else {
            (
                address.clone().with(Protocol::P2p(PeerId::random().into())),
                None,
            )
        };

    let mut i = 0u64;
    let mut begin = None;

    if let Some(peer_id) = peer_id {
        litep2p.add_known_address(peer_id, std::iter::once(address.clone()));

        begin = Some(Instant::now());
        bitswap
            .send_request(peer_id, vec![(cid, WantType::Have)])
            .await;
    } else {
        litep2p
            .dial_address(dial_address.clone())
            .await
            .context("connect error")?;
    }

    let mut timeout = pin!(sleep(TIMEOUT));

    loop {
        tokio::select! {
            () = &mut timeout => {
                return Err(anyhow!("request timed out, test failed"));
            },
            event = litep2p.next_event() => {
                match event {
                    Some(Litep2pEvent::DialFailure { address: _, error }) => {
                        if peer_id.is_none() {
                            match error {
                                DialError::NegotiationError(
                                    NegotiationError::PeerIdMismatch(_, detected_peer_id)
                                ) => {
                                    info!("remote peer ID: {detected_peer_id}");
                                    peer_id = Some(detected_peer_id);
                                    dial_address = address
                                        .clone()
                                        .with(Protocol::P2p(detected_peer_id.into()));
                                    litep2p.add_known_address(
                                        detected_peer_id,
                                        std::iter::once(dial_address.clone()),
                                    );
                                    //litep2p
                                    //    .dial_address(dial_address.clone())
                                    //    .await
                                    //    .context("connect error")?;

                                    begin = Some(Instant::now());
                                    bitswap.send_request(
                                        detected_peer_id,
                                        vec![(cid, WantType::Have)],
                                    )
                                    .await;

                                    timeout.as_mut().reset((Instant::now() + TIMEOUT).into());
                                },
                                _ => {
                                    return Err(error).context("dial failure")
                                }
                            }
                        } else {
                            return Err(error).context("dial failure")
                        }
                    },
                    Some(Litep2pEvent::ConnectionEstablished { peer, endpoint: _ })
                        if Some(peer) == peer_id =>
                    {
                        //begin = Some(Instant::now());
                        //bitswap.send_request(peer, vec![(cid, WantType::Have)]).await;
                    }
                    None => return Err(anyhow!("litep2p event loop terminated")),
                    _ => {},

                }
            },
            event = bitswap.next() => {
                match event {
                    Some(BitswapEvent::Response { peer, responses }) if Some(peer) == peer_id => {
                        if responses.len() != 1 {
                            warn!("ignoring response with invalid len {}", responses.len());
                            continue
                        }
                        if let Some(response) = responses.first() {
                            match response {
                                ResponseType::Presence { cid: received_cid, presence } => {
                                    if received_cid != &cid {
                                        return Err(anyhow!(
                                            "got unexpected CID from remote: {}",
                                            received_cid,
                                        ));
                                    }
                                    if !matches!(presence, BlockPresenceType::Have) {
                                        return Err(anyhow!(
                                            "remote doesn't have the requested CID",
                                        ));
                                    }

                                    bitswap.send_request(peer, vec![(cid, WantType::Block)]).await;
                                    timeout.as_mut().reset((Instant::now() + TIMEOUT).into());
                                },
                                ResponseType::Block { cid: received_cid, block: _ } => {
                                    if received_cid != &cid {
                                        return Err(anyhow!(
                                            "got unexpected CID from remote: {}",
                                            received_cid,
                                        ));
                                    }

                                    i += 1;
                                    if i < iterations {
                                        bitswap.send_request(
                                            peer,
                                            vec![(cid, WantType::Have)],
                                        )
                                        .await;
                                        timeout.as_mut().reset((Instant::now() + TIMEOUT).into());
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    },
                    Some(BitswapEvent::Response { peer, .. }) => {
                        warn!("response from unknown peer {peer}")
                    },
                    Some(BitswapEvent::Request { peer, .. }) => {
                        warn!("unexpected request from remote peer {peer}");
                    },
                    None => return Err(anyhow!("bitswap event loop terminated")),
                }
            }
        }
    }

    let Some(begin) = begin else {
        return Err(anyhow!("begin time not known; this is a bug"));
    };

    let duration = Instant::now() - begin;
    let duration_sec = (duration.as_nanos() as f64) / 1e9;
    let rps = (iterations as f64) / duration_sec;

    info!("Test took {:.3} secs", duration_sec);
    info!("RPS: {:.0}", rps);

    Ok(())
}
