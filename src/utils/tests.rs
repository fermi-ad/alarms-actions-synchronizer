//! Utilities Module Tests

use super::*;

#[test]
fn should_get_command_topic() {
    assert_eq!("topicCommand", get_command_topic("topic"));
}
