use super::*;

#[test]
fn physical_limit_is_explicit_and_checked_without_a_queue_derived_default() {
    let config = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    assert!(config.native_limits.is_none());
    for invalid in [0, (isize::MAX as usize) + 1, usize::MAX] {
        assert!(config.clone().with_native_limits(invalid, 1).is_err());
    }
    for invalid in [0, (isize::MAX as usize) + 1, usize::MAX] {
        assert!(config.clone().with_native_limits(65536, invalid).is_err());
    }
    for valid in [1, 65536, isize::MAX as usize] {
        let selected = config.clone().with_native_limits(valid, 1).unwrap();
        assert_eq!(selected.native_limits.unwrap().backing_bytes.get(), valid);
        assert_eq!(bytes::BackingBudget::new(valid).unwrap().limit(), valid);
    }
}

#[test]
fn native_limits_reject_half_environment_before_any_device_factory() {
    use std::ffi::OsStr;
    let bytes = Some(OsStr::new("65536"));
    let slots = Some(OsStr::new("1"));
    assert!(NativeLimits::from_sources(None, None).unwrap().is_none());
    assert!(NativeLimits::from_sources(bytes, None).is_err());
    assert!(NativeLimits::from_sources(None, slots).is_err());
    for bad in ["", "0", "-1", "abc", "18446744073709551615"] {
        assert!(NativeLimits::from_sources(Some(OsStr::new(bad)), slots).is_err());
        assert!(NativeLimits::from_sources(bytes, Some(OsStr::new(bad))).is_err());
    }
    let selected = NativeLimits::from_sources(bytes, slots).unwrap().unwrap();
    assert_eq!(selected.backing_bytes.get(), 65536);
    assert_eq!(selected.device_slots.get(), 1);
}

#[cfg(unix)]
#[test]
fn native_limits_reject_non_utf8_without_legacy_fallback() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let invalid = Some(OsStr::from_bytes(&[0xff]));
    assert!(NativeLimits::from_sources(invalid, Some(OsStr::new("1"))).is_err());
    assert!(NativeLimits::from_sources(Some(OsStr::new("65536")), invalid).is_err());
}
