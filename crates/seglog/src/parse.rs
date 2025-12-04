use crate::{
    CRC32C_SIZE, COMPRESSION_FLAG, LEN_SIZE, LENGTH_MASK, RECORD_HEAD_SIZE, calculate_crc32c,
    read::{ReadError, is_truncation_marker},
};

/// Parses a record from a byte slice starting at the given offset.
///
/// This function extracts and validates a record from raw bytes without performing any I/O.
/// It validates the CRC32C checksum and automatically decompresses data if needed.
///
/// # Arguments
///
/// * `bytes` - The byte buffer containing the record
/// * `offset` - The starting position within the buffer to parse from
///
/// # Returns
///
/// Returns a tuple of `([u8; H], Vec<u8>, usize)` where:
/// - The first element is the header portion (H bytes)
/// - The second element is the decompressed data
/// - The third element is the total bytes consumed from the buffer
///
/// # Errors
///
/// - `ReadError::OutOfBounds` - If the buffer doesn't contain enough bytes
/// - `ReadError::TruncationMarker` - If a truncation marker (all zeros) is encountered
/// - `ReadError::Crc32cMismatch` - If the CRC32C checksum validation fails
/// - `ReadError::Io` - If decompression fails
pub fn parse_record<const H: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<([u8; H], Vec<u8>, usize), ReadError> {
    // Check if we have enough bytes for the record header
    if offset + RECORD_HEAD_SIZE > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE,
            flushed_offset: bytes.len() as u64,
        });
    }

    let record_header_buf = &bytes[offset..offset + RECORD_HEAD_SIZE];

    // Check for truncation marker
    if is_truncation_marker(record_header_buf) {
        return Err(ReadError::TruncationMarker {
            offset: offset as u64,
        });
    }

    // Parse record header
    let length_bytes: [u8; LEN_SIZE] = record_header_buf[..LEN_SIZE].try_into().unwrap();
    let length_with_flag = u32::from_le_bytes(length_bytes);
    let is_compressed = length_with_flag & COMPRESSION_FLAG != 0;
    let payload_len = (length_with_flag & LENGTH_MASK) as usize; // H + data_len
    let crc = u32::from_le_bytes(
        record_header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
            .try_into()
            .unwrap(),
    );

    let payload_offset = offset + RECORD_HEAD_SIZE;

    // Check if we have enough bytes for the payload
    if payload_offset + payload_len > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE + payload_len,
            flushed_offset: bytes.len() as u64,
        });
    }

    let payload = &bytes[payload_offset..payload_offset + payload_len];
    let header: [u8; H] = payload[..H].try_into().unwrap();
    let compressed_data = &payload[H..];

    // Validate CRC over header + compressed data
    let calculated_crc = calculate_crc32c(&length_bytes, &header, compressed_data);
    if crc != calculated_crc {
        return Err(ReadError::Crc32cMismatch {
            offset: offset as u64,
        });
    }

    // Decompress if needed
    let final_data = if is_compressed {
        if compressed_data.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed data too short to contain original size",
            )
            .into());
        }
        let original_size_bytes: [u8; 4] = compressed_data[..4].try_into().unwrap();
        let _original_size = u32::from_le_bytes(original_size_bytes) as usize;

        let mut decompressed = Vec::new();
        zstd::stream::copy_decode(&compressed_data[4..], &mut decompressed)?;
        decompressed
    } else {
        compressed_data.to_vec()
    };

    Ok((header, final_data, RECORD_HEAD_SIZE + payload_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_record<const H: usize>(header: &[u8; H], data: &[u8]) -> Vec<u8> {
        let payload_len = (H + data.len()) as u32;
        let payload_len_bytes = payload_len.to_le_bytes();
        let crc = calculate_crc32c(&payload_len_bytes, header, data);

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&payload_len_bytes);
        buffer.extend_from_slice(&crc.to_le_bytes());
        buffer.extend_from_slice(header);
        buffer.extend_from_slice(data);
        buffer
    }

    #[test]
    fn test_parse_simple_record() {
        let header = [];
        let data = b"hello world";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_empty_record() {
        let header = [];
        let data = b"";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE);
    }

    #[test]
    fn test_parse_record_at_offset() {
        let header = [];
        let data1 = b"first record";
        let data2 = b"second record";

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&create_record(&header, data1));
        let second_offset = buffer.len();
        buffer.extend_from_slice(&create_record(&header, data2));

        // Parse first record
        let (_, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data1);
        assert_eq!(size, RECORD_HEAD_SIZE + data1.len());

        // Parse second record
        let (_, parsed_data, size) = parse_record::<0>(&buffer, second_offset).unwrap();
        assert_eq!(parsed_data, data2);
        assert_eq!(size, RECORD_HEAD_SIZE + data2.len());
    }

    #[test]
    fn test_parse_with_header() {
        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"hello world";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<8>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + header.len() + data.len());
    }

    #[test]
    fn test_parse_truncation_marker() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE + 10];
        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::TruncationMarker { offset: 0 })
        ));
    }

    #[test]
    fn test_parse_insufficient_bytes_for_header() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE - 1];
        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 0,
                length: 8,
                flushed_offset: 7,
            })
        ));
    }

    #[test]
    fn test_parse_insufficient_bytes_for_data() {
        let header = [];
        let data = b"hello world";
        let mut buffer = create_record(&header, data);
        buffer.truncate(buffer.len() - 1); // Remove one byte of data

        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 0,
                length: 19,
                flushed_offset: 18,
            })
        ));
    }

    #[test]
    fn test_parse_invalid_crc() {
        let header = [];
        let data = b"hello world";
        let mut buffer = create_record(&header, data);

        // Corrupt the CRC
        buffer[LEN_SIZE] ^= 0xFF;

        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn test_parse_large_record() {
        let header = [];
        let data = vec![0x42u8; 1024 * 1024]; // 1 MB
        let buffer = create_record(&header, &data);

        let (_, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_multiple_records() {
        let header = [];
        let records = vec![
            b"first".as_slice(),
            b"second record".as_slice(),
            b"third".as_slice(),
        ];

        let mut buffer = Vec::new();
        let mut offsets = Vec::new();

        for data in &records {
            offsets.push(buffer.len());
            buffer.extend_from_slice(&create_record(&header, data));
        }

        // Parse all records
        for (i, data) in records.iter().enumerate() {
            let (_, parsed_data, size) = parse_record::<0>(&buffer, offsets[i]).unwrap();
            assert_eq!(parsed_data, *data);
            assert_eq!(size, RECORD_HEAD_SIZE + data.len());
        }
    }
}
