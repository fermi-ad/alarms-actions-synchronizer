//! Tests for the config module.

use super::*;

#[test]
fn config_load_error_missing_variable_displays_variable_name() {
    let err = ConfigLoadError::MissingVariable("SOME_VAR");
    assert_eq!(
        err.to_string(),
        "Required environment variable 'SOME_VAR' is not set"
    );
}

#[test]
fn config_load_error_missing_variable_is_equal_to_itself() {
    let err = ConfigLoadError::MissingVariable("SOME_VAR");
    assert_eq!(err, ConfigLoadError::MissingVariable("SOME_VAR"));
}

#[test]
fn config_load_error_missing_variable_is_not_equal_to_different_variable() {
    let a = ConfigLoadError::MissingVariable("VAR_A");
    let b = ConfigLoadError::MissingVariable("VAR_B");
    assert_ne!(a, b);
}

#[test]
fn logging_init_error_already_initialized_displays_message() {
    let err = LoggingInitError::AlreadyInitialized;
    assert_eq!(
        err.to_string(),
        "A global tracing subscriber has already been set"
    );
}

#[test]
fn logging_init_error_already_initialized_is_equal_to_itself() {
    assert_eq!(
        LoggingInitError::AlreadyInitialized,
        LoggingInitError::AlreadyInitialized
    );
}
