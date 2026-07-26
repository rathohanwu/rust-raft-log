use super::*;
use crate::storage::utils::create_new_memory_mapped_file;
use std::path::PathBuf;

/// Returns a path under the repository-local test artifact directory.
///
/// These tests exercise mmap-backed files, so keeping their artifacts out of
/// the caller's working directory avoids stray `.dat` files when Cargo is
/// launched from a parent directory.
fn test_segment_path(file_name: &str) -> String {
    let temp_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("temp");
    std::fs::create_dir_all(&temp_dir).expect("create test artifact directory");
    temp_dir.join(file_name).to_string_lossy().into_owned()
}

#[test]
fn should_return_rotated_needed_result() {
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-0000010.dat"), 67)
            .expect("should be opened the file");

    let mut log_segment = LogFileSegment::new(memory_map, 1);
    let result = log_segment.append_entry(&LogEntry::new_with_type(
        1,
        1,
        EntryType::Normal,
        "this is han1".as_bytes().to_vec(),
    ));

    match result {
        AppendResult::Success => panic!("should be rotation needed"),
        AppendResult::RotationNeeded => {}
    }
}

#[test]
fn should_return_success_result() {
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-0000011.dat"), 100)
            .expect("should be opened the file");

    let mut log_segment = LogFileSegment::new(memory_map, 1);
    let result = log_segment.append_entry(&LogEntry::new_with_type(
        1,
        1,
        EntryType::Normal,
        "this is han1".as_bytes().to_vec(),
    ));

    match result {
        AppendResult::Success => {}
        AppendResult::RotationNeeded => panic!("should be successful needed"),
    }
}

#[test]
fn should_return_correct_first_index_and_entry_count() {
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-0000001.dat"), 10_000)
            .expect("should be opened the file");

    let mut log_segment = LogFileSegment::new(memory_map, 1);
    assert_eq!(0, log_segment.get_entry_count());

    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        1,
        EntryType::Normal,
        "this is han1".as_bytes().to_vec(),
    ));
    assert_eq!(1, log_segment.get_entry_count());

    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        2,
        EntryType::Normal,
        "this is han2".as_bytes().to_vec(),
    ));
    assert_eq!(2, log_segment.get_entry_count());

    let log_entry_1 = log_segment.get_entry_at(1);
    verify_log_entry(log_entry_1, 1, 1, "this is han1");

    let log_entry_2 = log_segment.get_entry_at(2);
    verify_log_entry(log_entry_2, 1, 2, "this is han2");
}

#[test]
fn should_return_empty_entry_result() {
    // Given
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-0000002.dat"), 10_000)
            .expect("should be opened the file");
    let mut log_segment = LogFileSegment::new(memory_map, 8);

    // When & Then
    assert_eq!(0, log_segment.get_entry_count());
    verify_empty_log_entry(log_segment.get_entry_at(7));
    verify_empty_log_entry(log_segment.get_entry_at(8));

    // Given
    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        8,
        EntryType::Normal,
        "this is han8".as_bytes().to_vec(),
    ));
    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        9,
        EntryType::Normal,
        "this is han9".as_bytes().to_vec(),
    ));

    // When & Then
    assert_eq!(2, log_segment.get_entry_count());
    verify_log_entry(log_segment.get_entry_at(8), 1, 8, "this is han8");
    verify_log_entry(log_segment.get_entry_at(9), 1, 9, "this is han9");

    verify_empty_log_entry(log_segment.get_entry_at(10));
}

#[test]
fn should_truncate_log_correctly() {
    // Given
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-0000003.dat"), 10_000)
            .expect("should be opened the file");
    let mut log_segment = LogFileSegment::new(memory_map, 11);
    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        11,
        EntryType::Normal,
        "this is 11th data".as_bytes().to_vec(),
    ));
    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        12,
        EntryType::Normal,
        "this is 12th data".as_bytes().to_vec(),
    ));

    assert_eq!(2, log_segment.get_entry_count());
    verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
    verify_log_entry(log_segment.get_entry_at(12), 1, 12, "this is 12th data");

    assert_eq!(false, log_segment.truncate_from(13));
    assert_eq!(true, log_segment.truncate_from(12));
    assert_eq!(1, log_segment.get_entry_count());
    verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
    verify_empty_log_entry(log_segment.get_entry_at(12));

    log_segment.append_entry(&LogEntry::new_with_type(
        1,
        12,
        EntryType::Normal,
        "this is new 12th data".as_bytes().to_vec(),
    ));

    verify_log_entry(log_segment.get_entry_at(12), 1, 12, "this is new 12th data");
}

fn verify_log_entry(entry: Option<LogEntry>, term: u64, index: u64, payload: &str) {
    match entry {
        None => panic!("should be some log entry"),
        Some(entry) => {
            assert_eq!(term, entry.term);
            assert_eq!(index, entry.index);
            assert_eq!(payload.as_bytes().to_vec(), entry.payload);
        }
    }
}

fn verify_empty_log_entry(entry: Option<LogEntry>) {
    match entry {
        None => {}
        Some(_) => panic!("should be none log entry"),
    }
}

#[test]
fn test_entry_type_encoding_decoding() {
    let memory_map =
        create_new_memory_mapped_file(&test_segment_path("log-segment-entry-types.dat"), 1000)
            .expect("should be opened the file");

    let mut log_segment = LogFileSegment::new(memory_map, 1);

    // Test Normal entry type
    let normal_entry = LogEntry::new_with_type(
        1,
        1,
        EntryType::Normal,
        "normal command".as_bytes().to_vec(),
    );
    let result = log_segment.append_entry(&normal_entry);
    assert!(matches!(result, AppendResult::Success));

    // Test NoOp entry type
    let noop_entry = LogEntry::new_with_type(1, 2, EntryType::NoOp, vec![]);
    let result = log_segment.append_entry(&noop_entry);
    assert!(matches!(result, AppendResult::Success));

    // Verify entries can be retrieved with correct types
    let retrieved_normal = log_segment
        .get_entry_at(1)
        .expect("Should retrieve normal entry");
    assert_eq!(retrieved_normal.entry_type, EntryType::Normal);
    assert_eq!(retrieved_normal.term, 1);
    assert_eq!(retrieved_normal.index, 1);
    assert_eq!(retrieved_normal.payload, "normal command".as_bytes());

    let retrieved_noop = log_segment
        .get_entry_at(2)
        .expect("Should retrieve noop entry");
    assert_eq!(retrieved_noop.entry_type, EntryType::NoOp);
    assert_eq!(retrieved_noop.term, 1);
    assert_eq!(retrieved_noop.index, 2);
    assert_eq!(retrieved_noop.payload, Vec::<u8>::new());

    assert_eq!(log_segment.get_entry_count(), 2);
}

#[test]
fn test_entry_type_conversion() {
    // Test EntryType to u8 conversion
    assert_eq!(u8::from(EntryType::Normal), 0);
    assert_eq!(u8::from(EntryType::NoOp), 1);

    // Test u8 to EntryType conversion
    assert_eq!(EntryType::from(0), EntryType::Normal);
    assert_eq!(EntryType::from(1), EntryType::NoOp);
    assert_eq!(EntryType::from(255), EntryType::Normal); // Unknown values default to Normal
}

#[test]
fn test_calculate_total_size_with_entry_type() {
    let normal_entry = LogEntry::new_with_type(1, 1, EntryType::Normal, "test".as_bytes().to_vec());
    // term (8) + index (8) + entry_type (1) + payload_size (8) + payload (4) = 29
    assert_eq!(normal_entry.calculate_total_size(), 29);

    let noop_entry = LogEntry::new_with_type(1, 1, EntryType::NoOp, vec![]);
    // term (8) + index (8) + entry_type (1) + payload_size (8) + payload (0) = 25
    assert_eq!(noop_entry.calculate_total_size(), 25);
}
