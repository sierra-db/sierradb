use crate::{
    CRC32C_SIZE, LEN_SIZE, RECORD_HEAD_SIZE, calculate_crc32c,
    read::{ReadError, is_truncation_marker},
};

/// Parses a record from a byte slice starting at the given offset.
///
/// This function extracts and validates a record from raw bytes without performing any I/O.
/// It validates the CRC32C checksum to ensure data integrity.
///
/// # Arguments
///
/// * `bytes` - The byte buffer containing the record
/// * `offset` - The starting position within the buffer to parse from
///
/// # Returns
///
/// Returns a tuple of `(&[u8], usize)` where:
/// - The first element is the data portion of the record (excluding header)
/// - The second element is the total bytes consumed (header size + data length)
///
/// # Errors
///
/// - `ReadError::OutOfBounds` - If the buffer doesn't contain enough bytes for the header or data,
///   or if a truncation marker (all zeros) is encountered
/// - `ReadError::Crc32cMismatch` - If the CRC32C checksum validation fails
///
/// # Example
///
/// ```
/// use seglog::parse::parse_record;
/// use seglog::RECORD_HEAD_SIZE;
///
/// // Create a buffer with a record: 4-byte length + 4-byte CRC + data
/// let data = b"hello world";
/// let data_len = data.len() as u32;
/// let data_len_bytes = data_len.to_le_bytes();
/// let crc = seglog::calculate_crc32c(&data_len_bytes, data);
///
/// let mut buffer = Vec::new();
/// buffer.extend_from_slice(&data_len_bytes);
/// buffer.extend_from_slice(&crc.to_le_bytes());
/// buffer.extend_from_slice(data);
///
/// // Parse the record
/// let (parsed_data, total_size) = parse_record(&buffer, 0).unwrap();
/// assert_eq!(parsed_data, b"hello world");
/// assert_eq!(total_size, RECORD_HEAD_SIZE + data.len());
/// ```
pub fn parse_record(bytes: &[u8], offset: usize) -> Result<(&[u8], usize), ReadError> {
    // Check if we have enough bytes for the header
    if offset + RECORD_HEAD_SIZE > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE,
            flushed_offset: bytes.len() as u64,
        });
    }

    let header_buf = &bytes[offset..offset + RECORD_HEAD_SIZE];

    // Check for truncation marker
    if is_truncation_marker(header_buf) {
        return Err(ReadError::TruncationMarker {
            offset: offset as u64,
        });
    }

    // Parse header
    let data_len_bytes: [u8; LEN_SIZE] = header_buf[..LEN_SIZE].try_into().unwrap();
    let data_len = u32::from_le_bytes(data_len_bytes) as usize;
    let crc = u32::from_le_bytes(
        header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
            .try_into()
            .unwrap(),
    );

    let data_offset = offset + RECORD_HEAD_SIZE;

    // Check if we have enough bytes for the data
    if data_offset + data_len > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE + data_len,
            flushed_offset: bytes.len() as u64,
        });
    }

    let data = &bytes[data_offset..data_offset + data_len];

    // Validate CRC
    let calculated_crc = calculate_crc32c(&data_len_bytes, data);
    if crc != calculated_crc {
        return Err(ReadError::Crc32cMismatch {
            offset: offset as u64,
        });
    }

    Ok((data, RECORD_HEAD_SIZE + data_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_record(data: &[u8]) -> Vec<u8> {
        let data_len = data.len() as u32;
        let data_len_bytes = data_len.to_le_bytes();
        let crc = calculate_crc32c(&data_len_bytes, data);

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&data_len_bytes);
        buffer.extend_from_slice(&crc.to_le_bytes());
        buffer.extend_from_slice(data);
        buffer
    }

    #[test]
    fn test_parse_simple_record() {
        let data = b"hello world";
        let buffer = create_record(data);

        let (parsed_data, size) = parse_record(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_empty_record() {
        let data = b"";
        let buffer = create_record(data);

        let (parsed_data, size) = parse_record(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE);
    }

    #[test]
    fn test_parse_record_at_offset() {
        let data1 = b"first record";
        let data2 = b"second record";

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&create_record(data1));
        let second_offset = buffer.len();
        buffer.extend_from_slice(&create_record(data2));

        // Parse first record
        let (parsed_data, size) = parse_record(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data1);
        assert_eq!(size, RECORD_HEAD_SIZE + data1.len());

        // Parse second record
        let (parsed_data, size) = parse_record(&buffer, second_offset).unwrap();
        assert_eq!(parsed_data, data2);
        assert_eq!(size, RECORD_HEAD_SIZE + data2.len());
    }

    #[test]
    fn test_parse_truncation_marker() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE + 10];
        let result = parse_record(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 0,
                length: 8,
                flushed_offset: 18,
            })
        ));
    }

    #[test]
    fn test_parse_insufficient_bytes_for_header() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE - 1];
        let result = parse_record(&buffer, 0);
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
        let data = b"hello world";
        let mut buffer = create_record(data);
        buffer.truncate(buffer.len() - 1); // Remove one byte of data

        let result = parse_record(&buffer, 0);
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
        let data = b"hello world";
        let mut buffer = create_record(data);

        // Corrupt the CRC
        buffer[LEN_SIZE] ^= 0xFF;

        let result = parse_record(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn test_parse_large_record() {
        let data = vec![0x42u8; 1024 * 1024]; // 1 MB
        let buffer = create_record(&data);

        let (parsed_data, size) = parse_record(&buffer, 0).unwrap();
        assert_eq!(parsed_data, &data[..]);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_multiple_records() {
        let records = vec![
            b"first".as_slice(),
            b"second record".as_slice(),
            b"third".as_slice(),
        ];

        let mut buffer = Vec::new();
        let mut offsets = Vec::new();

        for data in &records {
            offsets.push(buffer.len());
            buffer.extend_from_slice(&create_record(data));
        }

        // Parse all records
        for (i, data) in records.iter().enumerate() {
            let (parsed_data, size) = parse_record(&buffer, offsets[i]).unwrap();
            assert_eq!(parsed_data, *data);
            assert_eq!(size, RECORD_HEAD_SIZE + data.len());
        }
    }
}
