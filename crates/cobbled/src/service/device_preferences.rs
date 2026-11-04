use super::*;

pub(super) fn validate_hrm_interval(
    code: u8,
    firmware: (u8, u8, u16),
) -> Result<HrMonitoringInterval, DaemonError> {
    let interval = HrMonitoringInterval::from_u8(code);
    if !interval.is_supported_by(firmware) {
        return Err(DaemonError::Failed(format!(
            "HRM measurement interval {code} is unsupported by this firmware"
        )));
    }
    Ok(interval)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_edits_require_supported_firmware_but_known_old_intervals_still_work() {
        assert!(validate_hrm_interval(4, (4, 38, 99)).is_err());
        assert_eq!(
            validate_hrm_interval(4, (4, 39, 0)).unwrap(),
            HrMonitoringInterval::FiveMin
        );
        assert!(validate_hrm_interval(17, (4, 44, 0)).is_err());
        assert_eq!(
            validate_hrm_interval(0, (4, 9, 146)).unwrap(),
            HrMonitoringInterval::TenMin
        );
        assert!(validate_hrm_interval(0, (4, 9, 145)).is_err());
    }
}
