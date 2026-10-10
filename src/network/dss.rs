use crate::network::codepoint::{self as cp, CodePoint};
use std::ops::Deref;
use std::ops::Range;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub(crate) enum DssReadError {
    Transport(std::io::Error),
    Protocol(String),
}

impl std::fmt::Display for DssReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::Protocol(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for DssReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Protocol(_) => None,
        }
    }
}

impl From<std::io::Error> for DssReadError {
    fn from(error: std::io::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<String> for DssReadError {
    fn from(message: String) -> Self {
        Self::Protocol(message)
    }
}

pub const DSS_MAGIC: u8 = 0xd0;
pub const DSS_TYPE_REQUEST: u8 = 1;
pub const DSS_TYPE_REPLY: u8 = 2;
pub const DSS_TYPE_OBJECT: u8 = 3;
pub const DSS_TYPE_COMM: u8 = 4;
pub const DSS_FLAG_CHAINED: u8 = 0x40;
pub const DSS_FLAG_ERROR: u8 = 0x20;
pub const DSS_FLAG_SAME_ID: u8 = 0x10;
pub(crate) const CONTINUATION: u16 = 0x8000;
pub(crate) const MAX_SEGMENT: usize = 0x7fff;
const MAX_VECTORED_SEGMENTS: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DssHeader {
    pub length: u16,
    pub kind: u8,
    pub chained: bool,
    pub same_id: bool,
    pub has_error: bool,
    pub correlation_id: u16,
}

#[derive(Debug)]
pub struct DssPayload {
    storage: Vec<u8>,
    range: Range<usize>,
}

impl DssPayload {
    fn from_storage(storage: Vec<u8>, range: Range<usize>) -> Self {
        Self { storage, range }
    }

    fn from_owned(payload: Vec<u8>) -> Self {
        let range = 0..payload.len();
        Self::from_storage(payload, range)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.range.clone()]
    }

    /// Returns an owned copy of the logical DDM payload.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    /// Consumes the payload, reusing its storage when it contains no DSS/DDM header.
    pub fn try_into_vec(self) -> Result<Vec<u8>, String> {
        let Self { storage, range } = self;
        if range.start == 0 && range.end == storage.len() {
            return Ok(storage);
        }
        let payload = &storage[range];
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(payload.len())
            .map_err(|error| format!("unable to reserve owned DDM payload: {error}"))?;
        owned.extend_from_slice(payload);
        Ok(owned)
    }
}

impl Clone for DssPayload {
    fn clone(&self) -> Self {
        Self::from_owned(self.to_vec())
    }
}

impl PartialEq for DssPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for DssPayload {}

impl Deref for DssPayload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[u8]> for DssPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const N: usize> PartialEq<[u8; N]> for DssPayload {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<Vec<u8>> for DssPayload {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DssPacket {
    pub header: DssHeader,
    pub codepoint: CodePoint,
    pub payload: DssPayload,
    pub more_data: bool,
}

impl DssPacket {
    /// Constructs a packet from an already isolated DDM payload.
    pub fn new(header: DssHeader, codepoint: CodePoint, payload: Vec<u8>, more_data: bool) -> Self {
        Self {
            header,
            codepoint,
            payload: DssPayload::from_owned(payload),
            more_data,
        }
    }

    /// Borrows the DDM payload from the packet's owned DSS buffer.
    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

pub fn build_dss_header(
    payload_len: usize,
    kind: u8,
    chained: bool,
    same_id: bool,
    has_error: bool,
    correlation_id: u16,
) -> [u8; 6] {
    let mut flags = kind & 0x0f;
    if chained {
        flags |= DSS_FLAG_CHAINED;
    }
    if same_id {
        flags |= DSS_FLAG_SAME_ID;
    }
    if has_error {
        flags |= DSS_FLAG_ERROR;
    }
    let mut output = [0; 6];
    output[..2].copy_from_slice(&((payload_len + 6) as u16).to_be_bytes());
    output[2] = DSS_MAGIC;
    output[3] = flags;
    output[4..].copy_from_slice(&correlation_id.to_be_bytes());
    output
}

pub fn encode_request_dss(
    payload: &[u8],
    current_id: u16,
    next_same_id: bool,
    last_packet: bool,
) -> Result<(Vec<u8>, u16), String> {
    let kind = request_kind(payload)?;
    let mut out = Vec::with_capacity(payload.len() + 12);
    let mut offset = 0;
    let mut first = true;
    while offset < payload.len() {
        let header_size = if first { 6 } else { 2 };
        let amount = (if first {
            MAX_SEGMENT - 6
        } else {
            MAX_SEGMENT - 2
        })
        .min(payload.len() - offset);
        let more = offset + amount < payload.len();
        let length = (header_size + amount) as u16 | if more { CONTINUATION } else { 0 };
        out.extend_from_slice(&length.to_be_bytes());
        if first {
            out.push(DSS_MAGIC);
            out.push(
                kind | if !last_packet { DSS_FLAG_CHAINED } else { 0 }
                    | if next_same_id { DSS_FLAG_SAME_ID } else { 0 },
            );
            out.extend_from_slice(&current_id.to_be_bytes());
        }
        out.extend_from_slice(&payload[offset..offset + amount]);
        offset += amount;
        first = false;
    }
    Ok((
        out,
        if next_same_id {
            current_id
        } else {
            current_id.wrapping_add(1)
        },
    ))
}

fn request_kind(payload: &[u8]) -> Result<u8, String> {
    if payload.len() < 4 {
        return Err("payload too short for DDM object".into());
    }
    let codepoint = u16::from_be_bytes([payload[2], payload[3]]);
    Ok(
        if [cp::SQLSTT.0, cp::SQLATTR.0, cp::SQLDTA.0, cp::EXTDTA.0].contains(&codepoint) {
            DSS_TYPE_OBJECT
        } else {
            DSS_TYPE_REQUEST
        },
    )
}

/// Writes request DSS framing around an existing DDM payload without copying it into a frame
/// buffer. The header and payload are passed separately so large requests avoid a full-size copy.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DssRequest<'a> {
    pub payload: &'a [u8],
    pub correlation_id: Option<u16>,
    pub next_same_id: bool,
    pub last_packet: bool,
}

pub(crate) async fn write_request_dss_chain<W>(
    writer: &mut W,
    requests: &[DssRequest<'_>],
    initial_id: u16,
) -> std::io::Result<u16>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    if requests.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "DSS request chain cannot be empty",
        ));
    }
    for request in requests {
        request_kind(request.payload)
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    }
    if let [request] = requests
        && request.payload.len() <= MAX_SEGMENT - 6
    {
        let kind = request_kind(request.payload)
            .expect("all DDM request payloads were validated before writing");
        let correlation_id = request.correlation_id.unwrap_or(initial_id);
        let mut header = [0u8; 6];
        let length = (header.len() + request.payload.len()) as u16;
        header[..2].copy_from_slice(&length.to_be_bytes());
        header[2] = DSS_MAGIC;
        header[3] =
            kind | if !request.last_packet {
                DSS_FLAG_CHAINED
            } else {
                0
            } | if request.next_same_id {
                DSS_FLAG_SAME_ID
            } else {
                0
            };
        header[4..].copy_from_slice(&correlation_id.to_be_bytes());
        write_vectored_header_payload(writer, &header, request.payload).await?;
        return Ok(if request.next_same_id {
            correlation_id
        } else {
            correlation_id.wrapping_add(1)
        });
    }

    let mut request_index = 0;
    let mut payload_offset = 0;
    let mut request_id = requests.first().map_or(initial_id, |request| {
        request.correlation_id.unwrap_or(initial_id)
    });
    let mut next_id = initial_id;
    let mut first = true;
    let mut headers = [[0u8; 6]; MAX_VECTORED_SEGMENTS];
    let mut header_sizes = [0usize; MAX_VECTORED_SEGMENTS];
    let mut payload_slices: [&[u8]; MAX_VECTORED_SEGMENTS] = [&[]; MAX_VECTORED_SEGMENTS];
    while request_index < requests.len() {
        let mut segment_count = 0;
        while segment_count < MAX_VECTORED_SEGMENTS && request_index < requests.len() {
            let request = &requests[request_index];
            let header_size = if first { 6 } else { 2 };
            let amount = (if first {
                MAX_SEGMENT - 6
            } else {
                MAX_SEGMENT - 2
            })
            .min(request.payload.len() - payload_offset);
            let more = payload_offset + amount < request.payload.len();
            let length = (header_size + amount) as u16 | if more { CONTINUATION } else { 0 };
            headers[segment_count][..2].copy_from_slice(&length.to_be_bytes());
            if first {
                let kind = request_kind(request.payload)
                    .expect("all DDM request payloads were validated before writing");
                headers[segment_count][2] = DSS_MAGIC;
                headers[segment_count][3] =
                    kind | if !request.last_packet {
                        DSS_FLAG_CHAINED
                    } else {
                        0
                    } | if request.next_same_id {
                        DSS_FLAG_SAME_ID
                    } else {
                        0
                    };
                headers[segment_count][4..].copy_from_slice(&request_id.to_be_bytes());
            }
            header_sizes[segment_count] = header_size;
            payload_slices[segment_count] =
                &request.payload[payload_offset..payload_offset + amount];
            payload_offset += amount;
            segment_count += 1;
            if payload_offset == request.payload.len() {
                next_id = if request.next_same_id {
                    request_id
                } else {
                    request_id.wrapping_add(1)
                };
                request_index += 1;
                payload_offset = 0;
                first = true;
                if let Some(next_request) = requests.get(request_index) {
                    request_id = next_request.correlation_id.unwrap_or(next_id);
                }
            } else {
                first = false;
            }
        }

        {
            let mut slices: [std::io::IoSlice<'_>; MAX_VECTORED_SEGMENTS * 2] =
                std::array::from_fn(|_| std::io::IoSlice::new(&[]));
            for index in 0..segment_count {
                slices[index * 2] = std::io::IoSlice::new(&headers[index][..header_sizes[index]]);
                slices[index * 2 + 1] = std::io::IoSlice::new(payload_slices[index]);
            }
            write_vectored_slices_all(writer, &mut slices[..segment_count * 2]).await?;
        }
    }
    Ok(next_id)
}

async fn write_vectored_header_payload<W>(
    writer: &mut W,
    header: &[u8; 6],
    payload: &[u8],
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut header_offset = 0;
    let mut payload_offset = 0;
    while header_offset < header.len() || payload_offset < payload.len() {
        let slices = [
            std::io::IoSlice::new(&header[header_offset..]),
            std::io::IoSlice::new(&payload[payload_offset..]),
        ];
        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write complete DSS segment",
            ));
        }
        let remaining_header = header.len() - header_offset;
        if written < remaining_header {
            header_offset += written;
        } else {
            header_offset = header.len();
            payload_offset += written - remaining_header;
        }
    }
    Ok(())
}

async fn write_vectored_slices_all<W>(
    writer: &mut W,
    slices: &mut [std::io::IoSlice<'_>],
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let mut index = 0;
    let mut slice_offset = 0;
    while index < slices.len() {
        let written = if slice_offset == 0 {
            writer.write_vectored(&slices[index..]).await?
        } else {
            let active_len = slices.len() - index;
            let mut active: [std::io::IoSlice<'_>; MAX_VECTORED_SEGMENTS * 2] =
                std::array::from_fn(|_| std::io::IoSlice::new(&[]));
            for active_index in 0..active_len {
                let source = &slices[index + active_index];
                active[active_index] = if active_index == 0 {
                    std::io::IoSlice::new(&source[slice_offset..])
                } else {
                    std::io::IoSlice::new(source)
                };
            }
            writer.write_vectored(&active[..active_len]).await?
        };
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write complete DSS segment",
            ));
        }
        let mut remaining = written;
        while remaining > 0 {
            let current_remaining = slices[index].len() - slice_offset;
            if remaining < current_remaining {
                slice_offset += remaining;
                remaining = 0;
            } else {
                remaining -= current_remaining;
                index += 1;
                slice_offset = 0;
            }
        }
    }
    Ok(())
}

#[cfg(feature = "bench-internals")]
pub async fn benchmark_write_request_dss<W>(writer: &mut W, payload: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let request = DssRequest {
        payload,
        correlation_id: Some(1),
        next_same_id: false,
        last_packet: true,
    };
    write_request_dss_chain(writer, &[request], 1)
        .await
        .map(|_| ())
}

#[cfg(feature = "bench-internals")]
pub async fn benchmark_write_three_request_chain<W>(
    writer: &mut W,
    first: &[u8],
    second: &[u8],
    third: &[u8],
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let requests = [
        DssRequest {
            payload: first,
            correlation_id: None,
            next_same_id: true,
            last_packet: false,
        },
        DssRequest {
            payload: second,
            correlation_id: None,
            next_same_id: false,
            last_packet: false,
        },
        DssRequest {
            payload: third,
            correlation_id: Some(1),
            next_same_id: false,
            last_packet: true,
        },
    ];
    write_request_dss_chain(writer, &requests, 1)
        .await
        .map(|_| ())
}

/// Compatibility name for the bounded LOB EXTDTA writer.
pub async fn write_external_blob_dss<W, R>(
    destination: &mut W,
    source: &mut R,
    data_length: u64,
    current_id: u16,
    next_same_id: bool,
    last_packet: bool,
) -> std::io::Result<u16>
where
    W: tokio::io::AsyncWrite + Unpin + ?Sized,
    R: AsyncRead + Unpin + ?Sized,
{
    write_external_lob_dss(
        destination,
        source,
        data_length,
        current_id,
        next_same_id,
        last_packet,
    )
    .await
}

/// Writes a LOB EXTDTA DDM object in bounded DSS segments from a forward-only reader.
pub async fn write_external_lob_dss<W, R>(
    destination: &mut W,
    source: &mut R,
    data_length: u64,
    current_id: u16,
    next_same_id: bool,
    last_packet: bool,
) -> std::io::Result<u16>
where
    W: tokio::io::AsyncWrite + Unpin + ?Sized,
    R: AsyncRead + Unpin + ?Sized,
{
    use tokio::io::AsyncWriteExt;

    let body_length = data_length.checked_add(1).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "LOB length overflows DRDA",
        )
    })?;
    let mut prefix = [0u8; 13];

    let ddm_header_length = if body_length
        .checked_add(4)
        .is_some_and(|length| length <= 0x7fff)
    {
        prefix[..2].copy_from_slice(&((body_length + 4) as u16).to_be_bytes());
        prefix[2..4].copy_from_slice(&cp::EXTDTA.0.to_be_bytes());
        4
    } else if body_length <= 0x7fff_ffff {
        prefix[..2].copy_from_slice(&0x8008u16.to_be_bytes());
        prefix[2..4].copy_from_slice(&cp::EXTDTA.0.to_be_bytes());
        prefix[4..8].copy_from_slice(&(body_length as u32).to_be_bytes());
        8
    } else {
        prefix[..2].copy_from_slice(&0x800cu16.to_be_bytes());
        prefix[2..4].copy_from_slice(&cp::EXTDTA.0.to_be_bytes());
        prefix[4..12].copy_from_slice(&body_length.to_be_bytes());
        12
    };
    prefix[ddm_header_length] = 0; // EXTDTA status byte.
    let prefix_length = ddm_header_length + 1;
    let mut prefix_offset = 0;
    let mut remaining = (prefix_length as u64)
        .checked_add(data_length)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "LOB length overflows DSS")
        })?;
    let mut first = true;
    let mut frame_storage = Vec::new();
    while remaining > 0 {
        let header_length = if first { 6 } else { 2 };
        let count = (MAX_SEGMENT - header_length).min(remaining.min(usize::MAX as u64) as usize);
        let more = (count as u64) < remaining;
        let frame_length = header_length + count;
        if frame_storage.len() < frame_length {
            frame_storage
                .try_reserve_exact(frame_length - frame_storage.len())
                .map_err(|error| {
                    std::io::Error::other(format!(
                        "unable to reserve DSS LOB write buffer: {error}"
                    ))
                })?;
            frame_storage.resize(frame_length, 0);
        }
        let frame = &mut frame_storage[..frame_length];
        let length = (frame.len() as u16) | if more { CONTINUATION } else { 0 };
        frame[..2].copy_from_slice(&length.to_be_bytes());
        if first {
            frame[2] = DSS_MAGIC;
            frame[3] = DSS_TYPE_OBJECT
                | if !last_packet { DSS_FLAG_CHAINED } else { 0 }
                | if next_same_id { DSS_FLAG_SAME_ID } else { 0 };
            frame[4..6].copy_from_slice(&current_id.to_be_bytes());
        }
        let mut written = 0;
        if prefix_offset < prefix_length {
            let take = count.min(prefix_length - prefix_offset);
            frame[header_length..header_length + take]
                .copy_from_slice(&prefix[prefix_offset..prefix_offset + take]);
            prefix_offset += take;
            written += take;
        }
        if written < count {
            source
                .read_exact(&mut frame[header_length + written..])
                .await?;
        }
        destination.write_all(frame).await?;
        remaining -= count as u64;
        first = false;
    }
    Ok(if next_same_id {
        current_id
    } else {
        current_id.wrapping_add(1)
    })
}

pub fn read_ddm_object(body: &[u8]) -> Result<(CodePoint, Vec<u8>), String> {
    let max_size = crate::config::ProtocolLimits::default().max_ddm_object_size;
    let (codepoint, payload_range) = read_ddm_object_range(body, max_size)?;
    Ok((codepoint, copy_ddm_payload(&body[payload_range])?))
}

fn read_ddm_object_range(
    body: &[u8],
    max_ddm_object_size: usize,
) -> Result<(CodePoint, Range<usize>), String> {
    if body.len() < 4 {
        return Err(format!(
            "invalid DDM object length: DSS body holds {} bytes",
            body.len()
        ));
    }
    let encoded = u16::from_be_bytes([body[0], body[1]]);
    let codepoint = CodePoint(u16::from_be_bytes([body[2], body[3]]));
    if encoded & 0x8000 == 0 {
        if encoded < 4 {
            return Err(format!("invalid DDM object length: {encoded}"));
        }
        let end = encoded as usize;
        if end > body.len() {
            return Err(format!(
                "DDM object length {end} exceeds DSS frame payload capacity {}",
                body.len()
            ));
        }
        if end - 4 > max_ddm_object_size {
            return Err(format!(
                "DDM object payload exceeds configured limit of {max_ddm_object_size} bytes"
            ));
        }
        return Ok((codepoint, 4..end));
    }
    let header_length = (encoded & 0x7fff) as usize;
    if !matches!(header_length, 4 | 8 | 12) {
        return Err(format!(
            "invalid DDM extended header length {header_length}"
        ));
    }
    let ext = header_length - 4;
    if ext == 0 {
        if body.len() - 4 > max_ddm_object_size {
            return Err(format!(
                "DDM object payload exceeds configured limit of {max_ddm_object_size} bytes"
            ));
        }
        return Ok((codepoint, 4..body.len()));
    }
    if header_length > body.len() {
        return Err(format!("invalid DDM extended length field 0x{encoded:04X}"));
    }
    let mut data_len = 0u64;
    for &byte in &body[4..4 + ext] {
        data_len = (data_len << 8) | byte as u64;
    }
    let start = 4 + ext;
    let data_len = usize::try_from(data_len)
        .map_err(|_| "DDM extended length exceeds the supported buffer size")?;
    if data_len > max_ddm_object_size {
        return Err(format!(
            "DDM object payload exceeds configured limit of {max_ddm_object_size} bytes"
        ));
    }
    if data_len as u128 > (body.len() - start) as u128 {
        return Err(format!(
            "DDM extended length {data_len} exceeds DSS payload {}",
            body.len() - start
        ));
    }
    Ok((codepoint, start..start + data_len))
}

fn copy_ddm_payload(payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(payload.len())
        .map_err(|error| format!("unable to reserve DDM payload: {error}"))?;
    owned.extend_from_slice(payload);
    Ok(owned)
}

pub async fn read_dss<R: AsyncRead + Unpin>(reader: &mut R) -> Result<DssPacket, String> {
    read_dss_typed(reader)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn read_dss_typed<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<DssPacket, DssReadError> {
    let limits = crate::config::ProtocolLimits::default();
    read_dss_typed_with_limits(reader, limits.max_dss_size, limits.max_ddm_object_size).await
}

pub(crate) async fn read_dss_typed_with_limits<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_dss_size: usize,
    max_ddm_object_size: usize,
) -> Result<DssPacket, DssReadError> {
    let mut raw = [0u8; 6];
    reader.read_exact(&mut raw).await.map_err(|error| {
        DssReadError::Transport(std::io::Error::new(
            error.kind(),
            format!("failed to read DSS header: {error}"),
        ))
    })?;
    if raw[2] != DSS_MAGIC {
        return Err(format!(
            "invalid DSS packet: missing 0xD0 magic identifier (got 0x{:02X})",
            raw[2]
        )
        .into());
    }
    let length = u16::from_be_bytes([raw[0], raw[1]]);
    let flags = raw[3];
    let header = DssHeader {
        length,
        kind: flags & 0x0f,
        chained: flags & DSS_FLAG_CHAINED != 0,
        same_id: flags & DSS_FLAG_SAME_ID != 0,
        has_error: flags & DSS_FLAG_ERROR != 0,
        correlation_id: u16::from_be_bytes([raw[4], raw[5]]),
    };
    let first_len = (length & !CONTINUATION) as usize;
    if first_len < 6 {
        return Err(
            format!("invalid DSS frame length {first_len}: less than header size 6").into(),
        );
    }
    if first_len > max_dss_size {
        return Err(format!("DSS packet exceeds configured limit of {max_dss_size} bytes").into());
    }
    let max_ddm_wire_size = max_ddm_object_size
        .checked_add(12)
        .ok_or_else(|| "DDM object limit overflow".to_string())?;
    let initial_body_size = first_len - 6;
    if initial_body_size > max_ddm_wire_size {
        return Err(
            format!("DDM object exceeds configured limit of {max_ddm_object_size} bytes").into(),
        );
    }
    let mut body = Vec::new();
    body.try_reserve_exact(initial_body_size).map_err(|error| {
        DssReadError::Protocol(format!("unable to reserve DSS receive buffer: {error}"))
    })?;
    body.resize(initial_body_size, 0);
    reader.read_exact(&mut body).await.map_err(|error| {
        DssReadError::Transport(std::io::Error::new(
            error.kind(),
            format!("failed to read DSS segment: {error}"),
        ))
    })?;
    let mut continued = length & CONTINUATION != 0;
    let mut packet_size = first_len;
    while continued {
        let mut part_header = [0; 2];
        reader.read_exact(&mut part_header).await.map_err(|error| {
            DssReadError::Transport(std::io::Error::new(
                error.kind(),
                format!("failed to read DSS continuation header: {error}"),
            ))
        })?;
        let part_len = u16::from_be_bytes(part_header);
        continued = part_len & CONTINUATION != 0;
        let n = (part_len & !CONTINUATION) as usize;
        if n < 2 {
            return Err(format!("invalid DSS continuation length {n}").into());
        }
        packet_size = packet_size
            .checked_add(n)
            .ok_or_else(|| "DSS packet length overflow".to_string())?;
        if packet_size > max_dss_size {
            return Err(
                format!("DSS packet exceeds configured limit of {max_dss_size} bytes").into(),
            );
        }
        let old = body.len();
        let appended = n - 2;
        let new_len = old
            .checked_add(appended)
            .ok_or_else(|| "DSS body length overflow".to_string())?;
        if new_len > max_ddm_wire_size {
            return Err(format!(
                "DDM object exceeds configured limit of {max_ddm_object_size} bytes"
            )
            .into());
        }
        body.try_reserve(appended).map_err(|error| {
            DssReadError::Protocol(format!("unable to grow DSS receive buffer: {error}"))
        })?;
        body.resize(new_len, 0);
        reader.read_exact(&mut body[old..]).await.map_err(|error| {
            DssReadError::Transport(std::io::Error::new(
                error.kind(),
                format!("failed to read DSS continuation: {error}"),
            ))
        })?;
    }
    let (codepoint, payload_range) = read_ddm_object_range(&body, max_ddm_object_size)?;
    Ok(DssPacket {
        header,
        codepoint,
        payload: DssPayload::from_storage(body, payload_range),
        more_data: false,
    })
}

pub fn encode_reply_dss(ddm: &[u8], chained: bool, correlation_id: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(ddm.len() + 12);
    let mut offset = 0;
    let mut first = true;
    while offset < ddm.len() {
        let hs = if first { 6 } else { 2 };
        let amount = (if first {
            MAX_SEGMENT - 6
        } else {
            MAX_SEGMENT - 2
        })
        .min(ddm.len() - offset);
        let more = offset + amount < ddm.len();
        let len = (hs + amount) as u16 | if more { CONTINUATION } else { 0 };
        out.extend_from_slice(&len.to_be_bytes());
        if first {
            out.push(DSS_MAGIC);
            out.push(DSS_TYPE_REPLY | if chained { DSS_FLAG_CHAINED } else { 0 });
            out.extend_from_slice(&correlation_id.to_be_bytes());
        }
        out.extend_from_slice(&ddm[offset..offset + amount]);
        offset += amount;
        first = false;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::ddm::pack_ddm_object;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncWrite, BufReader};

    struct FragmentedWriter {
        bytes: Vec<u8>,
        max_write: usize,
    }

    impl FragmentedWriter {
        fn write_fragments<'a>(&mut self, fragments: impl IntoIterator<Item = &'a [u8]>) -> usize {
            let mut written = 0;
            for fragment in fragments {
                let amount = (self.max_write - written).min(fragment.len());
                self.bytes.extend_from_slice(&fragment[..amount]);
                written += amount;
                if written == self.max_write {
                    break;
                }
            }
            written
        }
    }

    impl AsyncWrite for FragmentedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let amount = this.max_write.min(buffer.len());
            this.bytes.extend_from_slice(&buffer[..amount]);
            Poll::Ready(Ok(amount))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffers: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            Poll::Ready(Ok(
                this.write_fragments(buffers.iter().map(|buffer| &buffer[..]))
            ))
        }
    }

    #[tokio::test]
    async fn vectored_request_writer_matches_framing_with_partial_writes() {
        for size in [4usize, 32_761, 32_763, 65_535] {
            let mut payload = vec![0u8; size];
            payload[2..4].copy_from_slice(&cp::SQLDTA.0.to_be_bytes());
            payload[4..].fill(0x5a);
            for (same_id, last_packet, current_id) in [(false, true, 19), (true, false, 65_535)] {
                let (expected, expected_next) =
                    encode_request_dss(&payload, current_id, same_id, last_packet).unwrap();
                let mut writer = FragmentedWriter {
                    bytes: Vec::new(),
                    max_write: 3,
                };
                let request = DssRequest {
                    payload: &payload,
                    correlation_id: Some(current_id),
                    next_same_id: same_id,
                    last_packet,
                };
                let next = write_request_dss_chain(&mut writer, &[request], current_id)
                    .await
                    .unwrap();
                assert_eq!(writer.bytes, expected, "payload length {size}");
                assert_eq!(next, expected_next);
            }
        }
    }

    #[tokio::test]
    async fn vectored_request_chain_matches_sequential_packet_encoding() {
        let execute = pack_ddm_object(cp::EXCSQLSTT, &[0x31; 12]);
        let sql_dta = pack_ddm_object(cp::SQLDTA, &[0x5a; 600_000]);
        let commit = crate::network::ddm::pack_commit();
        let requests = [
            DssRequest {
                payload: &execute,
                correlation_id: None,
                next_same_id: true,
                last_packet: false,
            },
            DssRequest {
                payload: &sql_dta,
                correlation_id: None,
                next_same_id: false,
                last_packet: false,
            },
            DssRequest {
                payload: &commit,
                correlation_id: Some(1),
                next_same_id: false,
                last_packet: true,
            },
        ];
        let (mut expected, _) = encode_request_dss(&execute, 1, true, false).unwrap();
        let (sql_dta, _) = encode_request_dss(&sql_dta, 1, false, false).unwrap();
        let (commit, expected_next) = encode_request_dss(&commit, 1, false, true).unwrap();
        expected.extend_from_slice(&sql_dta);
        expected.extend_from_slice(&commit);

        let mut writer = FragmentedWriter {
            bytes: Vec::new(),
            max_write: 100_000,
        };
        let next_id = write_request_dss_chain(&mut writer, &requests, 1)
            .await
            .unwrap();
        assert_eq!(writer.bytes, expected);
        assert_eq!(next_id, expected_next);
    }

    #[tokio::test]
    async fn vectored_request_writer_rejects_an_empty_chain() {
        let mut writer = FragmentedWriter {
            bytes: Vec::new(),
            max_write: 3,
        };
        let error = write_request_dss_chain(&mut writer, &[], 1)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(writer.bytes.is_empty());
    }

    #[tokio::test]
    async fn request_and_reply_frames_round_trip_across_segments() {
        let small = pack_ddm_object(cp::EXCSAT, &[0xde, 0xad, 0xbe, 0xef]);
        let (bytes, next) = encode_request_dss(&small, 1, false, true).unwrap();
        assert_eq!(next, 2);
        let mut reader = BufReader::new(std::io::Cursor::new(bytes));
        let packet = read_dss(&mut reader).await.unwrap();
        assert_eq!(packet.header.kind, DSS_TYPE_REQUEST);
        assert_eq!(packet.header.correlation_id, 1);
        assert_eq!(packet.codepoint, cp::EXCSAT);
        assert_eq!(packet.payload, [0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(packet.payload.range, 4..8);
        assert_eq!(
            packet.payload().as_ptr(),
            packet.payload.storage[4..].as_ptr()
        );
        let data = vec![b'x'; 200_000];
        let mut object = vec![0x80, 0x04];
        object.extend_from_slice(&cp::EXTDTA.0.to_be_bytes());
        object.extend_from_slice(&data);
        let reply = encode_reply_dss(&object, true, 7);
        let mut reader = BufReader::new(std::io::Cursor::new(reply));
        let packet = read_dss(&mut reader).await.unwrap();
        assert!(packet.header.chained);
        assert_eq!(packet.codepoint, cp::EXTDTA);
        assert_eq!(packet.payload, data);
    }

    #[tokio::test]
    async fn streamed_blob_frames_match_materialized_extdta_at_length_boundaries() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        for size in [0usize, 32_762, 32_763, 32_767, 32_768, 70_003] {
            let data = (0..size)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            let (mut writer, mut reader) = tokio::io::duplex(256 * 1024);
            let mut source = std::io::Cursor::new(data.clone());
            let next =
                write_external_blob_dss(&mut writer, &mut source, size as u64, 19, false, true)
                    .await
                    .unwrap();
            writer.shutdown().await.unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();

            let mut payload = vec![0];
            payload.extend_from_slice(&data);
            let ddm = pack_ddm_object(cp::EXTDTA, &payload);
            let (expected, expected_next) = encode_request_dss(&ddm, 19, false, true).unwrap();
            assert_eq!(actual, expected, "BLOB length {size}");
            assert_eq!(next, expected_next);
        }
    }

    #[tokio::test]
    async fn streamed_blob_sends_only_declared_bytes_and_leaves_reader_remainder_unread() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let mut source = std::io::Cursor::new(vec![0x11, 0x22, 0x33]);
        write_external_blob_dss(&mut writer, &mut source, 2, 19, false, true)
            .await
            .unwrap();
        assert_eq!(source.position(), 2);
        writer.shutdown().await.unwrap();

        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        let ddm = pack_ddm_object(cp::EXTDTA, &[0, 0x11, 0x22]);
        let (expected, _) = encode_request_dss(&ddm, 19, false, true).unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn rejects_bad_magic_and_short_frame_lengths() {
        let mut reader = BufReader::new(std::io::Cursor::new(vec![0, 10, 0xaa, 1, 0, 1]));
        assert!(
            read_dss(&mut reader)
                .await
                .unwrap_err()
                .to_string()
                .contains("0xD0")
        );
        let mut reader = BufReader::new(std::io::Cursor::new(vec![0, 4, 0xd0, 1, 0, 1]));
        assert!(
            read_dss(&mut reader)
                .await
                .unwrap_err()
                .to_string()
                .contains("less than header")
        );
    }

    #[tokio::test]
    async fn rejects_dss_and_ddm_objects_over_limits_and_invalid_extended_headers() {
        let object = pack_ddm_object(cp::SQLDTA, &[1, 2, 3, 4]);
        let reply = encode_reply_dss(&object, false, 1);
        let mut reader = BufReader::new(std::io::Cursor::new(reply.clone()));
        let error = read_dss_typed_with_limits(&mut reader, reply.len() - 1, 64)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DSS packet exceeds configured limit")
        );

        let mut reader = BufReader::new(std::io::Cursor::new(reply));
        let error = read_dss_typed_with_limits(&mut reader, 128, 3)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DDM object payload exceeds configured limit")
        );

        let endless_continuation = [
            0x80,
            0x06,
            DSS_MAGIC,
            DSS_TYPE_REPLY,
            0,
            1,
            0x80,
            0x02,
            0x80,
            0x02,
        ];
        let mut reader = BufReader::new(std::io::Cursor::new(endless_continuation));
        let error = read_dss_typed_with_limits(&mut reader, 8, 64)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("DSS packet exceeds configured limit")
        );

        assert!(read_ddm_object(&[0x80, 0x03, 0x24, 0x13]).is_err());
    }

    #[tokio::test]
    async fn classifies_eof_as_transport_and_malformed_frames_as_protocol_errors() {
        use crate::error::{Db2Error, Db2ErrorKind};

        let mut eof = BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let transport_error = Db2Error::from(read_dss_typed(&mut eof).await.unwrap_err());
        assert_eq!(transport_error.kind, Db2ErrorKind::Transport);
        assert!(
            transport_error
                .message
                .contains("failed to read DSS header")
        );

        let mut malformed = BufReader::new(std::io::Cursor::new(vec![0, 10, 0xaa, 1, 0, 1]));
        let protocol_error = Db2Error::from(read_dss_typed(&mut malformed).await.unwrap_err());
        assert_eq!(protocol_error.kind, Db2ErrorKind::Protocol);
        assert!(protocol_error.message.contains("0xD0"));
    }
}
