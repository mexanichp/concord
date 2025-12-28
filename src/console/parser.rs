// SPDX-License-Identifier: GPL-3.0+

use std::marker::PhantomData;
use std::str::FromStr;

pub struct Parser<TConfig: Default> {
  rules: Vec<Box<dyn RuleApplier<TConfig>>>,
  _marker: TConfig,
}

impl<TConfig: Default> Parser<TConfig> {
  fn new() -> Self {
    Self {
      rules: vec![],
      _marker: Default::default(),
    }
  }

  pub fn add_rule(&mut self, rule: Box<dyn RuleApplier<TConfig>>) -> &mut Self {
    self.rules.push(rule);

    self
  }

  pub fn generate(&self, input: &str) -> TConfig {
    let mut config = TConfig::default();

    for rule in self.rules.iter() {
      rule.apply(&mut config, input);
    }

    config
  }
}

pub struct Rule<TConfig, Value, Extractor>
where
  Extractor: Fn(&mut TConfig, Value),
{
  key: String,
  extractor: Extractor,
  _marker: PhantomData<(TConfig, Value)>,
}

impl<TConfig, Value, Extractor> Rule<TConfig, Value, Extractor>
where
  Extractor: Fn(&mut TConfig, Value),
{
  pub fn new(key: String, extractor: Extractor) -> Self {
    Self {
      key,
      extractor,
      _marker: Default::default(),
    }
  }
}

pub trait RuleApplier<TConfig> {
  fn apply(&self, config: &mut TConfig, input: &str);
}

impl<TConfig, Value: FromStr, Extractor> RuleApplier<TConfig>
  for Rule<TConfig, Value, Extractor>
where
  Extractor: Fn(&mut TConfig, Value),
{
  fn apply(&self, config: &mut TConfig, input: &str) {
    let key = format!(" {} ", self.key.trim());
    let index = input.find(key.as_str());
    match index {
      None => { /* ignore */ }
      Some(mut index) => {
        // TODO:MK: extract logic to its own function and add handling for values with whitespace "-c 'my long var'"
        // TODO:MK: add extra support for -c=123, -c "123 123"
        // TODO:MK: error handling
        // TODO:MK: command parsing
        let mut value = String::new();
        index += key.len();
        let mut chars = input.chars().skip(index);
        while let Some(char) = chars.next() {
          if char.is_whitespace() {
            continue;
          }

          value.push(char);
          for char in chars.by_ref() {
            if char.is_whitespace() {
              break;
            }
            value.push(char);
          }

          break;
        }

        let value = Value::from_str(&value);
        match value {
          Ok(extracted) => self.extractor.call((config, extracted)),
          Err(_) => {
            panic!("Can't parse value with key {:?}.", self.key)
          }
        }
      }
    }
  }
}

mod tests {
  use super::*;

  #[derive(Default, Debug)]
  struct MyConfig {
    size: i32,
    scenario: String,
  }

  #[test]
  pub fn parser_extracts_i32_rule_correctly() {
    let mut parser = Parser::<MyConfig>::new();

    parser.add_rule(Box::new(Rule::new(
      "-s".to_string(),
      |config: &mut MyConfig, value: i32| config.size = value,
    )));

    let config = parser.generate("test -s 12");

    assert_eq!(config.size, 12);
  }

  #[test]
  pub fn parser_extracts_string_correctly() {
    let mut parser = Parser::<MyConfig>::new();

    parser.add_rule(Box::new(Rule::new(
      "-s".to_string(),
      |config: &mut MyConfig, value: String| config.scenario = value,
    )));

    let config = parser.generate("test -s 12");

    assert_eq!(config.scenario, "12");
  }

  #[test]
  pub fn parser_extracts_multiple_rules_correctly() {
    let mut parser = Parser::<MyConfig>::new();

    parser.add_rule(Box::new(Rule::new(
      "-s".to_string(),
      |config: &mut MyConfig, value| config.size = value,
    )));

    parser.add_rule(Box::new(Rule::new(
      "--scenario".to_string(),
      |config: &mut MyConfig, value| config.scenario = value,
    )));

    let config = parser.generate("test -s 12 --scenario my_scenario /tmp/extra");

    assert_eq!(config.size, 12);
    assert_eq!(config.scenario, "my_scenario");
  }

  #[test]
  pub fn parser_extracts_partial_rules_correctly() {
    let mut parser = Parser::<MyConfig>::new();

    parser.add_rule(Box::new(Rule::new(
      "-s".to_string(),
      |config: &mut MyConfig, value| config.size = value,
    )));

    let config = parser.generate("test --scenario my_scenario -s 12 /tmp/extra");

    assert_eq!(config.size, 12);
    assert_eq!(config.scenario, String::default());
  }
}
