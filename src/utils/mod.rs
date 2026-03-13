//! Utilities Module
//!
//! Provides utility functions used throughout the application.

#[cfg(test)]
pub mod test_runner;

#[cfg(test)]
mod tests;

/// Generates the name of the command topic associated with the provided base topic.
///
/// Essentially, appends "Command" to the end of the topic name.
///
/// Returns a new instance, leaving the old reference intact.
pub fn get_command_topic(topic: &str) -> String {
    format!("{topic}Command")
}
