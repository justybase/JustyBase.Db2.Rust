//! DRDA USRENCPWD (SECMEC 7) and the unset-mechanism SERVER_ENCRYPT follow,
//! driven against an in-process mock DRDA server.
use justybase_db2::converters::ebcdic::{decode_cp500, encode_cp500};
use justybase_db2::network::codepoint::{self as cp, CodePoint};
use justybase_db2::network::{ddm, dss};
use justybase_db2::{Config, Db2ErrorKind, Db2Session};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SERVER_PRIVATE_HEX: &str = "1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a";

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect()
}

fn server_private() -> [u8; 32] {
    from_hex(SERVER_PRIVATE_HEX).try_into().unwrap()
}

fn server_public() -> Vec<u8> {
    justybase_db2::security::calculate_public_key(&server_private())
        .unwrap()
        .to_vec()
}

fn des_decrypt(key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Vec<u8> {
    use des::cipher::{Block, BlockDecrypt, KeyInit};
    let cipher = des::Des::new_from_slice(key).unwrap();
    let mut previous = [0u8; 8];
    previous.copy_from_slice(iv);
    let mut output = Vec::new();
    for chunk in ciphertext.as_chunks::<8>().0 {
        let mut block = Block::<des::Des>::clone_from_slice(chunk);
        let incoming = block;
        cipher.decrypt_block(&mut block);
        for (index, byte) in block.iter_mut().enumerate() {
            *byte ^= previous[index];
        }
        previous.copy_from_slice(&incoming);
        output.extend_from_slice(&block);
    }
    let padding = *output.last().unwrap() as usize;
    output.truncate(output.len() - padding);
    output
}

async fn read_chain(socket: &mut TcpStream) -> Vec<dss::DssPacket> {
    let mut result = Vec::new();
    loop {
        let packet = dss::read_dss(socket).await.unwrap();
        let chained = packet.header.chained;
        result.push(packet);
        if !chained {
            return result;
        }
    }
}

async fn send(socket: &mut TcpStream, codepoint: CodePoint, payload: &[u8], chained: bool) {
    let object = ddm::pack_ddm_object(codepoint, payload);
    socket
        .write_all(&dss::encode_reply_dss(&object, chained, 1))
        .await
        .unwrap();
}

fn codepoints(chain: &[dss::DssPacket]) -> Vec<CodePoint> {
    chain.iter().map(|packet| packet.codepoint).collect()
}

fn params(payload: &[u8]) -> HashMap<CodePoint, Vec<u8>> {
    ddm::parse_ddm_parameters(payload).unwrap()
}

fn u16_field(fields: &HashMap<CodePoint, Vec<u8>>, codepoint: CodePoint) -> u16 {
    let value = &fields[&codepoint];
    u16::from_be_bytes([value[0], value[1]])
}

fn u16_list(codepoint: CodePoint, values: &[u16]) -> Vec<u8> {
    let body: Vec<u8> = values
        .iter()
        .flat_map(|value| value.to_be_bytes())
        .collect();
    ddm::pack_bytes(codepoint, &body)
}

fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
    parts.concat()
}

fn sql_card() -> [u8; 54] {
    let mut result = [0; 54];
    result[5..10].copy_from_slice(b"00000");
    result
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// True when the client sends anything more before closing (or before a quiet second passes).
async fn client_sends_more(socket: &mut TcpStream) -> bool {
    let mut byte = [0];
    matches!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte)).await,
        Ok(Ok(count)) if count > 0
    )
}

async fn complete_login(socket: &mut TcpStream) {
    send(
        socket,
        cp::SECCHKRM,
        &ddm::pack_bytes(cp::SECCHKCD, &[0]),
        true,
    )
    .await;
    send(
        socket,
        cp::ACCRDBRM,
        &ddm::pack_bytes(cp::RDBINTTKN, &[4, 28, 167, 50]),
        false,
    )
    .await;
    let setup = read_chain(socket).await;
    assert_eq!(
        codepoints(&setup),
        vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
    );
    send(socket, cp::SQLCARD, &sql_card(), false).await;
}

async fn mock_server<F, Fut>(handler: F) -> (u16, tokio::task::JoinHandle<()>)
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        handler(socket).await;
    });
    (port, server)
}

fn config(port: u16, mechanism: u16) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port,
        database: "TESTDB".into(),
        user: "DB2INST1".into(),
        password: "clear-password".into(),
        timeout_ms: 3000,
        security_mechanism: mechanism,
        ..Config::default()
    }
}

async fn connect_error(config: Config) -> justybase_db2::Db2Error {
    match Db2Session::connect(config).await {
        Ok(_) => panic!("the connection must fail"),
        Err(error) => error,
    }
}

#[test]
fn secmec7_builders_carry_encalg_and_password_only_in_sectkn() {
    let accsec =
        ddm::pack_accsec("TESTDB", cp::SecurityMechanism::USRENCPWD, &[3, 4], Some(1)).unwrap();
    let accsec = params(&accsec[4..]);
    assert_eq!(u16_field(&accsec, cp::SECMEC), 7);
    assert_eq!(u16_field(&accsec, cp::ENCALG), 1);
    let plain = ddm::pack_accsec("TESTDB", cp::SecurityMechanism::USRIDPWD, &[], None).unwrap();
    assert!(!params(&plain[4..]).contains_key(&cp::ENCALG));

    let ciphertext = [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80];
    let secchk = ddm::pack_secchk_usrencpwd("TESTDB", "db2inst1", &ciphertext).unwrap();
    let secchk = params(&secchk[4..]);
    assert_eq!(u16_field(&secchk, cp::SECMEC), 7);
    assert_eq!(decode_cp500(&secchk[&cp::USRID]), "db2inst1");
    // Db2 LUW requires the encrypted password in SECTKN; PASSWORD is rejected (SECCHKCD=16).
    assert_eq!(secchk[&cp::SECTKN], ciphertext);
    assert!(!secchk.contains_key(&cp::PASSWORD));
    assert!(!secchk.contains_key(&cp::ENCALG));
}

#[test]
fn secmec7_iv_is_the_cp500_user_id_padded_or_truncated_to_one_block() {
    let client_private = justybase_db2::security::generate_private_key().unwrap();
    let client_public = justybase_db2::security::calculate_public_key(&client_private).unwrap();
    let server_key =
        justybase_db2::security::calculate_session_key(&client_public, &server_private()).unwrap();
    for user in ["U1", "DB2INST1", "AVERYLONGUSERNAME"] {
        let user_id = encode_cp500(user).unwrap();
        let ciphertext = justybase_db2::security::encrypt_usrencpwd(
            &encode_cp500("SecretPass123").unwrap(),
            &user_id,
            &server_public(),
            &client_private,
        )
        .unwrap();
        let mut iv = [0u8; 8];
        let length = user_id.len().min(8);
        iv[..length].copy_from_slice(&user_id[..length]);
        assert_eq!(
            decode_cp500(&des_decrypt(&server_key[12..20], &iv, &ciphertext)),
            "SecretPass123"
        );
    }
    assert!(
        justybase_db2::security::encrypt_usrencpwd(&[1], &[], &server_public(), &client_private)
            .is_err()
    );
}

#[tokio::test]
async fn secmec7_sends_the_encrypted_password_in_secchk_sectkn() {
    for echo_encalg in [true, false] {
        let (port, server) = mock_server(move |mut socket| async move {
            let opening = read_chain(&mut socket).await;
            assert_eq!(codepoints(&opening), vec![cp::EXCSAT, cp::ACCSEC]);
            let request = params(&opening[1].payload);
            assert_eq!(u16_field(&request, cp::SECMEC), 7);
            assert_eq!(u16_field(&request, cp::ENCALG), 1);
            let client_public = request[&cp::SECTKN].clone();
            assert_eq!(client_public.len(), 32);

            // Db2 LUW SERVER_ENCRYPT reply: SECMEC=7, SECTKN(32), ENCALG=1.
            let mut reply = ddm::pack_u16(cp::SECMEC, 7);
            reply.extend(ddm::pack_bytes(cp::SECTKN, &server_public()));
            if echo_encalg {
                reply.extend(ddm::pack_u16(cp::ENCALG, 1));
            }
            send(&mut socket, cp::ACCSECRD, &reply, false).await;

            let authentication = read_chain(&mut socket).await;
            assert_eq!(codepoints(&authentication), vec![cp::SECCHK, cp::ACCRDB]);
            assert!(
                !contains(
                    &authentication[0].payload,
                    &encode_cp500("clear-password").unwrap()
                ),
                "the plaintext password must not appear anywhere in SECCHK"
            );
            let secchk = params(&authentication[0].payload);
            assert_eq!(u16_field(&secchk, cp::SECMEC), 7);
            assert_eq!(decode_cp500(&secchk[&cp::USRID]), "DB2INST1");
            let server_key =
                justybase_db2::security::calculate_session_key(&client_public, &server_private())
                    .unwrap();
            let mut iv = [0u8; 8];
            iv.copy_from_slice(&encode_cp500("DB2INST1").unwrap());
            let password = des_decrypt(&server_key[12..20], &iv, &secchk[&cp::SECTKN]);
            assert_eq!(decode_cp500(&password), "clear-password");

            complete_login(&mut socket).await;
        })
        .await;
        let mut session = Db2Session::connect(config(port, 7)).await.unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn secmec3_against_server_encrypt_fails_with_the_accepted_list_and_no_secchk() {
    let (port, server) = mock_server(|mut socket| async move {
        let opening = read_chain(&mut socket).await;
        let request = params(&opening[1].payload);
        assert_eq!(u16_field(&request, cp::SECMEC), 3);
        assert!(!request.contains_key(&cp::SECTKN));
        assert!(!request.contains_key(&cp::ENCALG));
        // Captured from Db2 LUW AUTHENTICATION=SERVER_ENCRYPT: one SECMEC parameter
        // listing 7, 9, 10, SECCHKCD=1 and an ENCALG list; no server SECTKN.
        let reply = concat(&[
            u16_list(cp::SECMEC, &[7, 9, 10]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
            u16_list(cp::ENCALG, &[1, 2]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(
            !client_sends_more(&mut socket).await,
            "client must not send SECCHK, plaintext or otherwise, after a rejected ACCSEC"
        );
    })
    .await;
    let error = connect_error(config(port, 3)).await;
    assert_eq!(error.kind, Db2ErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("rejected security mechanism 3 (SECCHKCD=1); the server accepts 7, 9, 10"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_against_a_server_only_server_fails_with_the_accepted_list() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        // Captured from Db2 LUW AUTHENTICATION=SERVER: SECMEC list 3, 5 and SECCHKCD=1.
        let reply = concat(&[
            u16_list(cp::SECMEC, &[3, 5]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(
            !client_sends_more(&mut socket).await,
            "client must not downgrade to mechanism 3"
        );
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert!(
        error
            .to_string()
            .contains("rejected security mechanism 7 (SECCHKCD=1); the server accepts 3, 5"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_rejects_a_malformed_accsecrd_mechanism_list() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        send(
            &mut socket,
            cp::ACCSECRD,
            &ddm::pack_bytes(cp::SECMEC, &[0, 7, 0]),
            false,
        )
        .await;
        assert!(!client_sends_more(&mut socket).await);
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert_eq!(error.kind, Db2ErrorKind::Protocol);
    assert!(
        error
            .to_string()
            .contains("malformed security mechanism list"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_rejects_aes_selection_without_sending_secchk() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            ddm::pack_u16(cp::SECMEC, 7),
            ddm::pack_u16(cp::ENCALG, 2),
            ddm::pack_bytes(cp::SECTKN, &server_public()),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(
            !client_sends_more(&mut socket).await,
            "client must fail before SECCHK rather than sending credentials"
        );
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert_eq!(error.kind, Db2ErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("ENCALG=2; this driver supports DES (ENCALG=1) only"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_rejects_a_missing_server_token_before_secchk() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[ddm::pack_u16(cp::SECMEC, 7), ddm::pack_u16(cp::ENCALG, 1)]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(!client_sends_more(&mut socket).await);
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert!(
        error
            .to_string()
            .contains("did not return a usable server security token"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_surfaces_a_non_zero_accsecrd_secchkcd() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            ddm::pack_u16(cp::SECMEC, 7),
            ddm::pack_bytes(cp::SECCHKCD, &[0x13]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(!client_sends_more(&mut socket).await);
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert_eq!(error.kind, Db2ErrorKind::AuthenticationFailed);
    assert!(error.to_string().contains("SECCHKCD=19"), "{error}");
    server.await.unwrap();
}

#[tokio::test]
async fn secmec7_authentication_failure_does_not_retry_with_mechanism_3() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            ddm::pack_u16(cp::SECMEC, 7),
            ddm::pack_bytes(cp::SECTKN, &server_public()),
            ddm::pack_u16(cp::ENCALG, 1),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        let authentication = read_chain(&mut socket).await;
        assert_eq!(codepoints(&authentication), vec![cp::SECCHK, cp::ACCRDB]);
        assert_eq!(
            u16_field(&params(&authentication[0].payload), cp::SECMEC),
            7
        );
        // Db2 answers a wrong password with SECCHKCD=0x0F.
        send(
            &mut socket,
            cp::SECCHKRM,
            &ddm::pack_bytes(cp::SECCHKCD, &[0x0f]),
            false,
        )
        .await;
        assert!(
            !client_sends_more(&mut socket).await,
            "client must close after failed encrypted authentication without a mechanism 3 retry"
        );
    })
    .await;
    let error = connect_error(config(port, 7)).await;
    assert_eq!(error.kind, Db2ErrorKind::AuthenticationFailed);
    assert!(error.to_string().contains("SECCHKCD=15"), "{error}");
    server.await.unwrap();
}

// No configured mechanism (security_mechanism 0): the driver requests 3 and, when a
// SERVER_ENCRYPT server refuses it, sends a second ACCSEC on the same connection
// with the strongest encrypting mechanism the server listed.
#[tokio::test]
async fn unset_mechanism_follows_server_encrypt_to_an_encrypting_mechanism() {
    for (listed, expected) in [(vec![7u16, 9, 10], 9u16), (vec![7, 10], 7)] {
        let (port, server) = mock_server(move |mut socket| async move {
            let opening = read_chain(&mut socket).await;
            assert_eq!(codepoints(&opening), vec![cp::EXCSAT, cp::ACCSEC]);
            assert_eq!(u16_field(&params(&opening[1].payload), cp::SECMEC), 3);
            let reply = concat(&[
                u16_list(cp::SECMEC, &listed),
                ddm::pack_bytes(cp::SECCHKCD, &[1]),
                u16_list(cp::ENCALG, &[1, 2]),
            ]);
            send(&mut socket, cp::ACCSECRD, &reply, false).await;

            let retry = read_chain(&mut socket).await;
            assert_eq!(
                codepoints(&retry),
                vec![cp::ACCSEC],
                "the retry is a lone ACCSEC, not a new EXCSAT or a SECCHK"
            );
            let request = params(&retry[0].payload);
            assert_eq!(u16_field(&request, cp::SECMEC), expected);
            assert_eq!(request[&cp::SECTKN].len(), 32);
            let reply = concat(&[
                ddm::pack_u16(cp::SECMEC, expected),
                ddm::pack_bytes(cp::SECTKN, &server_public()),
                ddm::pack_u16(cp::ENCALG, 1),
            ]);
            send(&mut socket, cp::ACCSECRD, &reply, false).await;

            let authentication = read_chain(&mut socket).await;
            assert_eq!(codepoints(&authentication), vec![cp::SECCHK, cp::ACCRDB]);
            assert!(
                !contains(
                    &authentication[0].payload,
                    &encode_cp500("clear-password").unwrap()
                ),
                "the plaintext password must not appear in SECCHK"
            );
            let secchk = params(&authentication[0].payload);
            assert_eq!(u16_field(&secchk, cp::SECMEC), expected);
            assert!(!secchk.contains_key(&cp::PASSWORD));
            assert_eq!(
                secchk.contains_key(&cp::USRID),
                expected == 7,
                "only SECMEC 7 sends a clear user ID"
            );
            complete_login(&mut socket).await;
        })
        .await;
        let mut session = Db2Session::connect(config(port, 0)).await.unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn unset_mechanism_does_not_follow_a_server_listing_no_encrypting_mechanism() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            u16_list(cp::SECMEC, &[10, 11]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(
            !client_sends_more(&mut socket).await,
            "no second ACCSEC and no SECCHK"
        );
    })
    .await;
    let error = connect_error(config(port, 0)).await;
    assert!(
        error
            .to_string()
            .contains("rejected security mechanism 3 (SECCHKCD=1); the server accepts 10, 11"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn unset_mechanism_stops_when_the_server_rejects_the_mechanism_it_listed() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            u16_list(cp::SECMEC, &[7, 9]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        read_chain(&mut socket).await;
        let reply = concat(&[
            u16_list(cp::SECMEC, &[3]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        assert!(
            !client_sends_more(&mut socket).await,
            "no third ACCSEC and no SECCHK"
        );
    })
    .await;
    let error = connect_error(config(port, 0)).await;
    assert_eq!(error.kind, Db2ErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("listed security mechanism 9 as accepted but then rejected it"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn switch_user_fallback_stays_refused_after_an_automatic_switch() {
    let (port, server) = mock_server(|mut socket| async move {
        read_chain(&mut socket).await;
        let reply = concat(&[
            u16_list(cp::SECMEC, &[7, 9, 10]),
            ddm::pack_bytes(cp::SECCHKCD, &[1]),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        read_chain(&mut socket).await;
        let reply = concat(&[
            ddm::pack_u16(cp::SECMEC, 9),
            ddm::pack_bytes(cp::SECTKN, &server_public()),
        ]);
        send(&mut socket, cp::ACCSECRD, &reply, false).await;
        read_chain(&mut socket).await;
        complete_login(&mut socket).await;
        for _ in 0..2 {
            read_chain(&mut socket).await;
            let mut card = sql_card();
            card[1..5].copy_from_slice(&(-551i32).to_le_bytes());
            card[5..10].copy_from_slice(b"42501");
            send(&mut socket, cp::SQLCARD, &card, false).await;
        }
        assert!(
            !client_sends_more(&mut socket).await,
            "the plaintext SECCHK fallback must stay refused after switching to mechanism 9"
        );
    })
    .await;
    let mut session = Db2Session::connect(config(port, 0)).await.unwrap();
    let error = session
        .switch_user_with_password("db2test2", "secret")
        .await
        .unwrap_err();
    assert_eq!(error.kind, Db2ErrorKind::Unsupported);
    server.await.unwrap();
    drop(session);
}
