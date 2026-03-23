//! Sensor quantities as newtypes with **construct-time** validation (`Result`, no panic on bad input).
//! Wire payloads from the network are validated at the GCS boundary before use.

use std::fmt;

// -----------------------------------------------------------------------------
// Structured errors (Week 9: rich failure cases, no panic in RT paths)
// -----------------------------------------------------------------------------

/// Value outside the allowed physical range for a quantity (e.g. °C for temperature).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OutOfRange {
    pub quantity: &'static str,
    pub raw: f64,
    pub min: f64,
    pub max: f64,
}

impl fmt::Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} out of range: raw={} (valid {}..={})",
            self.quantity, self.raw, self.min, self.max
        )
    }
}

impl std::error::Error for OutOfRange {}

/// Payload too short to read a sensor bundle from the wire layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WirePayloadTooShort {
    pub actual: usize,
    pub need: usize,
}

impl fmt::Display for WirePayloadTooShort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "wire payload too short: len={} need>={}",
            self.actual, self.need
        )
    }
}

impl std::error::Error for WirePayloadTooShort {}

/// Validation failure at the network boundary (GCS) or construction (OCS).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SensorValidationError {
    TooShort(WirePayloadTooShort),
    OutOfRange(OutOfRange),
}

impl fmt::Display for SensorValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SensorValidationError::TooShort(e) => e.fmt(f),
            SensorValidationError::OutOfRange(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for SensorValidationError {}

// -----------------------------------------------------------------------------
// Newtypes (unit mismatch prevention at compile time)
// -----------------------------------------------------------------------------

/// Temperature in degrees Celsius (satellite thermal sensor).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Temperature(f64);

impl Temperature {
    /// Lecture / assignment style bounds for defensive validation.
    pub const MIN_C: f64 = -50.0;
    pub const MAX_C: f64 = 150.0;

    pub fn new(raw: f64) -> Result<Self, OutOfRange> {
        if raw.is_finite() && raw >= Self::MIN_C && raw <= Self::MAX_C {
            Ok(Self(raw))
        } else {
            Err(OutOfRange {
                quantity: "Temperature",
                raw,
                min: Self::MIN_C,
                max: Self::MAX_C,
            })
        }
    }

    #[inline]
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// IMU scalar sample (simulated; bounds wide enough for lab data).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Velocity(f64);

impl Velocity {
    pub const MIN: f64 = -1_000.0;
    pub const MAX: f64 = 1_000.0;

    pub fn new(raw: f64) -> Result<Self, OutOfRange> {
        if raw.is_finite() && raw >= Self::MIN && raw <= Self::MAX {
            Ok(Self(raw))
        } else {
            Err(OutOfRange {
                quantity: "Velocity",
                raw,
                min: Self::MIN,
                max: Self::MAX,
            })
        }
    }

    #[inline]
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// Power / bus voltage (volts).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Voltage(f64);

impl Voltage {
    pub const MIN_V: f64 = 0.0;
    pub const MAX_V: f64 = 10.0;

    pub fn new(raw: f64) -> Result<Self, OutOfRange> {
        if raw.is_finite() && raw >= Self::MIN_V && raw <= Self::MAX_V {
            Ok(Self(raw))
        } else {
            Err(OutOfRange {
                quantity: "Voltage",
                raw,
                min: Self::MIN_V,
                max: Self::MAX_V,
            })
        }
    }

    #[inline]
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// One of the onboard sensor quantities (distinct types → no mixing °C with V).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SensorQuantity {
    Temperature(Temperature),
    Velocity(Velocity),
    Voltage(Voltage),
}

impl SensorQuantity {
    #[inline]
    pub fn as_f64(self) -> f64 {
        match self {
            SensorQuantity::Temperature(t) => t.as_f64(),
            SensorQuantity::Velocity(v) => v.as_f64(),
            SensorQuantity::Voltage(v) => v.as_f64(),
        }
    }

    /// Build from raw simulation / ADC reading according to sensor id (0=thermal, 1=imu, 2=power).
    pub fn try_from_raw(sensor_id: u8, raw: f64) -> Result<Self, OutOfRange> {
        match sensor_id {
            0 => Temperature::new(raw).map(SensorQuantity::Temperature),
            1 => Velocity::new(raw).map(SensorQuantity::Velocity),
            2 => Voltage::new(raw).map(SensorQuantity::Voltage),
            _ => Temperature::new(raw).map(SensorQuantity::Temperature),
        }
    }

    /// Minimum bytes: fault(1) + read_at(8) + value(8) = 17. Optional `sensor_id` at index 25 if len >= 26.
    pub fn try_from_wire_payload(payload: &[u8]) -> Result<Self, SensorValidationError> {
        const MIN_LEN: usize = 17;
        if payload.len() < MIN_LEN {
            return Err(SensorValidationError::TooShort(WirePayloadTooShort {
                actual: payload.len(),
                need: MIN_LEN,
            }));
        }
        let mut vbytes = [0u8; 8];
        vbytes.copy_from_slice(&payload[9..17]);
        let raw = f64::from_le_bytes(vbytes);
        let sid = if payload.len() >= 26 {
            payload[25]
        } else {
            0
        };
        Self::try_from_raw(sid, raw).map_err(SensorValidationError::OutOfRange)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temperature_ok_mid_range() {
        let t = Temperature::new(20.0).unwrap();
        assert!((t.as_f64() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn temperature_err_below_min() {
        let e = Temperature::new(-51.0).unwrap_err();
        assert_eq!(e.quantity, "Temperature");
        assert_eq!(e.raw, -51.0);
    }

    #[test]
    fn temperature_rejects_nan() {
        assert!(Temperature::new(f64::NAN).is_err());
    }

    #[test]
    fn wire_payload_too_short() {
        let err = SensorQuantity::try_from_wire_payload(&[0_u8; 10]).unwrap_err();
        assert!(matches!(err, SensorValidationError::TooShort(_)));
    }

    #[test]
    fn wire_payload_ok_without_sensor_id_byte_defaults_thermal() {
        let mut p = vec![0_u8; 25];
        p[9..17].copy_from_slice(&20.0f64.to_le_bytes());
        let q = SensorQuantity::try_from_wire_payload(&p).unwrap();
        match q {
            SensorQuantity::Temperature(t) => assert!((t.as_f64() - 20.0).abs() < f64::EPSILON),
            _ => panic!("expected thermal"),
        }
    }
}
