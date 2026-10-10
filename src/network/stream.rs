//! Incremental DSS/DDM payload reader. The packet borrows its transport until drained.
use super::{codepoint::CodePoint, dss::DssHeader};
use crate::{Db2Error, Db2ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct Db2DssStream<'a, R> {
    reader: &'a mut R,
    pub header: DssHeader,
    pub codepoint: CodePoint,
    remaining: Option<u64>,
    segment_remaining: usize,
    continued: bool,
    segments: usize,
    total: u64,
    limit: u64,
    finished: bool,
}
impl<'a, R: AsyncRead + Unpin> Db2DssStream<'a, R> {
    pub async fn open(reader: &'a mut R, limit: u64) -> Result<Self, Db2Error> {
        if limit == 0 {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                "DSS payload limit must be positive",
            ));
        }
        let mut raw = [0; 6];
        reader.read_exact(&mut raw).await?;
        let length = u16::from_be_bytes([raw[0], raw[1]]);
        if raw[2] != 0xd0 || length & 0x7fff < 6 {
            return Err(Db2Error::protocol("Invalid DSS header"));
        }
        let header = DssHeader {
            length,
            kind: raw[3] & 15,
            chained: raw[3] & 64 != 0,
            same_id: raw[3] & 16 != 0,
            has_error: raw[3] & 32 != 0,
            correlation_id: u16::from_be_bytes([raw[4], raw[5]]),
        };
        let mut packet = Self {
            reader,
            header,
            codepoint: CodePoint(0),
            remaining: None,
            segment_remaining: (length & 0x7fff) as usize - 6,
            continued: length & 0x8000 != 0,
            segments: 1,
            total: 0,
            limit,
            finished: false,
        };
        let mut ddm = [0; 4];
        packet.read_header_bytes(&mut ddm).await?;
        packet.codepoint = CodePoint(u16::from_be_bytes([ddm[2], ddm[3]]));
        let encoded = u16::from_be_bytes([ddm[0], ddm[1]]);
        if encoded & 0x8000 == 0 {
            if encoded < 4 {
                return Err(Db2Error::protocol("Invalid DDM length"));
            }
            packet.remaining = Some((encoded - 4) as u64);
        } else {
            let width = (encoded & 0x7fff)
                .checked_sub(4)
                .ok_or_else(|| Db2Error::protocol("Invalid extended DDM length"))?
                as usize;
            if width > 8 {
                return Err(Db2Error::protocol("Invalid extended DDM width"));
            }
            if width > 0 {
                let mut raw = [0; 8];
                packet.read_header_bytes(&mut raw[8 - width..]).await?;
                packet.remaining = Some(u64::from_be_bytes(raw));
            }
        }
        if packet.remaining.is_some_and(|length| length > limit) {
            return Err(Db2Error::resource_limit(
                "DDM payload exceeds streamed limit",
            ));
        }
        Ok(packet)
    }
    async fn next_segment(&mut self) -> Result<bool, Db2Error> {
        if self.segment_remaining > 0 {
            return Ok(true);
        }
        if !self.continued {
            return Ok(false);
        }
        self.segments += 1;
        if self.segments > 65536 {
            return Err(Db2Error::resource_limit(
                "DSS continuation count exceeds limit",
            ));
        }
        let mut raw = [0; 2];
        self.reader.read_exact(&mut raw).await?;
        let length = u16::from_be_bytes(raw);
        if length & 0x7fff <= 2 {
            return Err(Db2Error::protocol("Empty or invalid DSS continuation"));
        }
        self.continued = length & 0x8000 != 0;
        self.segment_remaining = (length & 0x7fff) as usize - 2;
        Ok(true)
    }
    async fn read_header_bytes(&mut self, mut target: &mut [u8]) -> Result<(), Db2Error> {
        while !target.is_empty() {
            if !self.next_segment().await? {
                return Err(Db2Error::protocol("Truncated DDM header"));
            }
            let count = target.len().min(self.segment_remaining);
            self.reader.read_exact(&mut target[..count]).await?;
            self.segment_remaining -= count;
            target = &mut target[count..];
        }
        Ok(())
    }
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Db2Error> {
        if self.finished {
            return Ok(None);
        }
        if self.remaining == Some(0) {
            if self.next_segment().await? {
                return Err(Db2Error::protocol("Trailing DSS bytes after DDM payload"));
            }
            self.finished = true;
            return Ok(None);
        }
        if !self.next_segment().await? {
            if self.remaining.is_some_and(|n| n != 0) {
                return Err(Db2Error::protocol("Truncated DDM payload"));
            }
            self.finished = true;
            return Ok(None);
        }
        let count = self
            .segment_remaining
            .min(crate::resources::CHUNK_BYTES)
            .min(self.remaining.unwrap_or(u64::MAX).min(usize::MAX as u64) as usize);
        if count as u64 > self.limit - self.total {
            return Err(Db2Error::resource_limit(
                "DSS payload exceeds streamed limit",
            ));
        }
        let mut chunk = vec![0; count];
        self.reader.read_exact(&mut chunk).await?;
        self.segment_remaining -= count;
        self.total += count as u64;
        if let Some(remaining) = &mut self.remaining {
            *remaining -= count as u64;
        }
        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn continued_payload_is_incremental_and_matches_legacy_bytes() {
        let payload = vec![0xab; 100_000];
        let mut ddm = vec![0x80, 8, 0x14, 0x6c];
        ddm.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        ddm.extend_from_slice(&payload);
        let wire = super::super::dss::encode_reply_dss(&ddm, false, 42);
        let mut input = wire.as_slice();
        let mut packet = Db2DssStream::open(&mut input, 100_000).await.unwrap();
        assert_eq!(packet.codepoint, CodePoint(0x146c));
        assert_eq!(packet.header.correlation_id, 42);
        let mut actual = Vec::new();
        while let Some(chunk) = packet.next_chunk().await.unwrap() {
            assert!(chunk.len() <= 64 * 1024);
            actual.extend(chunk);
        }
        let _ = packet;
        assert!(input.is_empty());
        assert_eq!(actual, payload);
    }
    #[tokio::test]
    async fn declared_oversize_is_rejected_before_payload_read() {
        let wire = [0, 14, 0xd0, 2, 0, 1, 0x80, 8, 0x14, 0x6c, 0, 0, 1, 0];
        let mut input = wire.as_slice();
        let error = match Db2DssStream::open(&mut input, 255).await {
            Err(error) => error,
            Ok(_) => panic!("must reject"),
        };
        assert_eq!(error.kind, Db2ErrorKind::ResourceLimit);
    }
}
