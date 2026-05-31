use super::lexer::{Token, TokenKind};
use crate::ast::{
    ProtoDefinition, ProtoEnumAst, ProtoEnumValueAst, ProtoFieldAst, ProtoFileAst, ProtoImport,
    ProtoMessageAst, ProtoOptionAssignment, ProtoOptionLiteral, ProtoReservedRange, ProtoRpcAst,
    ProtoServiceAst, SourceSpan,
};

use super::ParseError;

pub(super) struct ProtoAstParser {
    tokens: Vec<Token>,
    pos: usize,
    file: String,
}

impl ProtoAstParser {
    pub(super) fn new(tokens: Vec<Token>, file: String) -> Self {
        Self {
            tokens,
            pos: 0,
            file,
        }
    }

    pub(super) fn parse(&mut self) -> Result<ProtoFileAst, ParseError> {
        let mut ast = ProtoFileAst {
            file: self.file.clone(),
            ..ProtoFileAst::default()
        };

        while self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("syntax") {
                self.consume();
                self.expect(TokenKind::Equal, "expected '=' after syntax")?;
                ast.syntax = self.read_scalar_value();
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().is_ident("package") {
                self.consume();
                ast.package = self.read_type_name();
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().is_ident("import") {
                ast.imports.push(self.parse_import()?);
            } else if self.cur().is_ident("option") {
                self.consume();
                ast.options.push(self.parse_option_assignment()?);
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().is_ident("message") {
                ast.definitions
                    .push(ProtoDefinition::Message(self.parse_message_ast()?));
            } else if self.cur().is_ident("enum") {
                ast.definitions
                    .push(ProtoDefinition::Enum(self.parse_enum_ast()?));
            } else if self.cur().is_ident("service") {
                ast.definitions
                    .push(ProtoDefinition::Service(self.parse_service_ast()?));
            } else {
                self.skip_to_statement_end();
            }
        }

        Ok(ast)
    }

    fn parse_import(&mut self) -> Result<ProtoImport, ParseError> {
        let span = self.span();
        self.consume();
        let mut weak = false;
        let mut public = false;
        if self.cur().is_ident("weak") {
            weak = true;
            self.consume();
        } else if self.cur().is_ident("public") {
            public = true;
            self.consume();
        }
        let path = if self.cur().kind == TokenKind::String {
            self.consume().value
        } else {
            return Err(self.syntax("expected import path string"));
        };
        self.consume_optional(TokenKind::Semicolon);
        Ok(ProtoImport {
            path,
            weak,
            public,
            span,
        })
    }

    fn parse_message_ast(&mut self) -> Result<ProtoMessageAst, ParseError> {
        let span = self.span();
        self.consume();
        let name = self
            .consume_ident()
            .ok_or_else(|| self.syntax("expected message name"))?;
        self.expect(TokenKind::LBrace, "expected '{' after message name")?;

        let mut out = ProtoMessageAst {
            name,
            span,
            ..ProtoMessageAst::default()
        };
        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("option") {
                self.consume();
                out.options.push(self.parse_option_assignment()?);
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().is_ident("message") {
                out.nested
                    .push(ProtoDefinition::Message(self.parse_message_ast()?));
            } else if self.cur().is_ident("enum") {
                out.nested
                    .push(ProtoDefinition::Enum(self.parse_enum_ast()?));
            } else if self.cur().is_ident("oneof") {
                self.consume();
                let oneof_group = self.consume_ident().unwrap_or_default();
                self.expect(TokenKind::LBrace, "expected '{' after oneof name")?;
                while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
                    if let Some(field) = self.parse_field_ast("", &oneof_group)? {
                        out.fields.push(field);
                    }
                }
                self.consume_optional(TokenKind::RBrace);
            } else if self.cur().is_ident("reserved") {
                let (ranges, names) = self.parse_reserved_ast();
                out.reserved_numbers.extend(ranges);
                out.reserved_names.extend(names);
            } else if self.cur().is_ident("extensions") {
                self.skip_to_statement_end();
            } else if self.cur().kind == TokenKind::Ident {
                if let Some(field) = self.parse_field_ast("", "")? {
                    out.fields.push(field);
                }
            } else {
                self.consume();
            }
        }
        self.consume_optional(TokenKind::RBrace);
        Ok(out)
    }

    fn parse_field_ast(
        &mut self,
        forced_label: &str,
        oneof_group: &str,
    ) -> Result<Option<ProtoFieldAst>, ParseError> {
        let span = self.span();
        let mut label = forced_label.to_string();
        if matches!(
            self.cur().value.as_str(),
            "optional" | "required" | "repeated"
        ) {
            label = self.consume().value;
        }
        let field_type = self.read_type_name();
        if field_type.is_empty() {
            self.skip_to_statement_end();
            return Ok(None);
        }
        let Some(name) = self.consume_ident() else {
            self.skip_to_statement_end();
            return Ok(None);
        };
        self.expect(TokenKind::Equal, "expected '=' after field name")?;
        let number = if self.cur().kind == TokenKind::Number {
            self.consume().value.parse::<i32>().unwrap_or_default()
        } else {
            0
        };
        let options = if self.cur().kind == TokenKind::LBracket {
            self.parse_bracket_options()?
        } else {
            Vec::new()
        };
        self.consume_optional(TokenKind::Semicolon);
        Ok(Some(ProtoFieldAst {
            label,
            field_type,
            name,
            number,
            oneof_group: oneof_group.to_string(),
            options,
            span,
        }))
    }

    fn parse_reserved_ast(&mut self) -> (Vec<ProtoReservedRange>, Vec<String>) {
        self.consume();
        let mut ranges = Vec::new();
        let mut names = Vec::new();
        while self.cur().kind != TokenKind::Semicolon && self.cur().kind != TokenKind::Eof {
            if self.cur().kind == TokenKind::Comma {
                self.consume();
                continue;
            }
            let span = self.span();
            match self.cur().kind {
                TokenKind::String => names.push(self.consume().value),
                TokenKind::Number => {
                    let start = self.consume().value.parse::<i32>().unwrap_or_default();
                    let mut end = start;
                    if self.consume_ident_if("to") {
                        end = if self.cur().is_ident("max") {
                            self.consume();
                            i32::MAX
                        } else if self.cur().kind == TokenKind::Number {
                            self.consume().value.parse::<i32>().unwrap_or(start)
                        } else {
                            start
                        };
                    }
                    ranges.push(ProtoReservedRange { start, end, span });
                }
                _ => {
                    self.consume();
                }
            }
        }
        self.consume_optional(TokenKind::Semicolon);
        (ranges, names)
    }

    fn parse_enum_ast(&mut self) -> Result<ProtoEnumAst, ParseError> {
        let span = self.span();
        self.consume();
        let name = self
            .consume_ident()
            .ok_or_else(|| self.syntax("expected enum name"))?;
        self.expect(TokenKind::LBrace, "expected '{' after enum name")?;

        let mut out = ProtoEnumAst {
            name,
            span,
            ..ProtoEnumAst::default()
        };
        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("option") {
                self.consume();
                out.options.push(self.parse_option_assignment()?);
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().kind == TokenKind::Ident {
                let value_span = self.span();
                let value_name = self.consume().value;
                if self.cur().kind == TokenKind::Equal {
                    self.consume();
                }
                let number = if self.cur().kind == TokenKind::Number {
                    self.consume().value.parse::<i32>().unwrap_or_default()
                } else {
                    0
                };
                if self.cur().kind == TokenKind::LBracket {
                    self.skip_bracket_block();
                }
                self.consume_optional(TokenKind::Semicolon);
                out.values.push(ProtoEnumValueAst {
                    name: value_name,
                    number,
                    span: value_span,
                });
            } else {
                self.consume();
            }
        }
        self.consume_optional(TokenKind::RBrace);
        Ok(out)
    }

    fn parse_service_ast(&mut self) -> Result<ProtoServiceAst, ParseError> {
        let span = self.span();
        self.consume();
        let name = self
            .consume_ident()
            .ok_or_else(|| self.syntax("expected service name"))?;
        self.expect(TokenKind::LBrace, "expected '{' after service name")?;

        let mut out = ProtoServiceAst {
            name,
            span,
            ..ProtoServiceAst::default()
        };
        while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
            if self.cur().is_ident("option") {
                self.consume();
                out.options.push(self.parse_option_assignment()?);
                self.consume_optional(TokenKind::Semicolon);
            } else if self.cur().is_ident("rpc") {
                out.rpcs.push(self.parse_rpc_ast()?);
            } else {
                self.skip_to_statement_end();
            }
        }
        self.consume_optional(TokenKind::RBrace);
        Ok(out)
    }

    fn parse_rpc_ast(&mut self) -> Result<ProtoRpcAst, ParseError> {
        let span = self.span();
        self.consume();
        let name = self
            .consume_ident()
            .ok_or_else(|| self.syntax("expected rpc name"))?;
        self.expect(TokenKind::LParen, "expected '(' after rpc name")?;
        let client_streaming = self.consume_ident_if("stream");
        let request_type = self.read_type_name();
        self.expect(TokenKind::RParen, "expected ')' after rpc request type")?;
        if !self.consume_ident_if("returns") {
            return Err(self.syntax("expected returns clause"));
        }
        self.expect(TokenKind::LParen, "expected '(' after returns")?;
        let server_streaming = self.consume_ident_if("stream");
        let response_type = self.read_type_name();
        self.expect(TokenKind::RParen, "expected ')' after rpc response type")?;
        let options = if self.cur().kind == TokenKind::LBrace {
            self.consume();
            let mut values = Vec::new();
            while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
                if self.cur().is_ident("option") {
                    self.consume();
                    values.push(self.parse_option_assignment()?);
                    self.consume_optional(TokenKind::Semicolon);
                } else {
                    self.skip_to_statement_end();
                }
            }
            self.consume_optional(TokenKind::RBrace);
            values
        } else {
            self.consume_optional(TokenKind::Semicolon);
            Vec::new()
        };
        Ok(ProtoRpcAst {
            name,
            request_type,
            response_type,
            client_streaming,
            server_streaming,
            options,
            span,
        })
    }

    fn parse_bracket_options(&mut self) -> Result<Vec<ProtoOptionAssignment>, ParseError> {
        self.expect(TokenKind::LBracket, "expected '[' before field options")?;
        let mut out = Vec::new();
        while self.cur().kind != TokenKind::RBracket && self.cur().kind != TokenKind::Eof {
            if self.cur().kind == TokenKind::Comma {
                self.consume();
                continue;
            }
            out.push(self.parse_option_assignment()?);
            self.consume_optional(TokenKind::Comma);
        }
        self.consume_optional(TokenKind::RBracket);
        Ok(out)
    }

    fn parse_option_assignment(&mut self) -> Result<ProtoOptionAssignment, ParseError> {
        let span = self.span();
        let name = self.read_option_name();
        self.expect(TokenKind::Equal, "expected '=' after option name")?;
        let value = self.parse_option_literal()?;
        Ok(ProtoOptionAssignment { name, value, span })
    }

    fn parse_option_literal(&mut self) -> Result<ProtoOptionLiteral, ParseError> {
        if self.cur().kind == TokenKind::LBrace {
            self.consume();
            let mut values = Vec::new();
            while self.cur().kind != TokenKind::RBrace && self.cur().kind != TokenKind::Eof {
                if self.cur().kind == TokenKind::Comma || self.cur().kind == TokenKind::Semicolon {
                    self.consume();
                    continue;
                }
                let span = self.span();
                let name = self
                    .consume_ident()
                    .ok_or_else(|| self.syntax("expected option object key"))?;
                self.consume_optional(TokenKind::Colon);
                values.push(ProtoOptionAssignment {
                    name,
                    value: self.parse_option_literal()?,
                    span,
                });
                self.consume_optional(TokenKind::Comma);
                self.consume_optional(TokenKind::Semicolon);
            }
            self.consume_optional(TokenKind::RBrace);
            Ok(ProtoOptionLiteral::Object(values))
        } else if self.cur().kind == TokenKind::LBracket {
            self.consume();
            let mut values = Vec::new();
            while self.cur().kind != TokenKind::RBracket && self.cur().kind != TokenKind::Eof {
                values.push(self.parse_option_literal()?);
                self.consume_optional(TokenKind::Comma);
            }
            self.consume_optional(TokenKind::RBracket);
            Ok(ProtoOptionLiteral::List(values))
        } else {
            Ok(ProtoOptionLiteral::Scalar(self.read_scalar_value()))
        }
    }

    fn read_option_name(&mut self) -> String {
        let mut out = String::new();
        if self.cur().kind == TokenKind::LParen {
            out.push('(');
            self.consume();
            while matches!(self.cur().kind, TokenKind::Ident | TokenKind::Dot) {
                out.push_str(&self.consume().value);
            }
            self.consume_optional(TokenKind::RParen);
            out.push(')');
        } else {
            while matches!(self.cur().kind, TokenKind::Ident | TokenKind::Dot) {
                out.push_str(&self.consume().value);
            }
        }
        out
    }

    fn read_type_name(&mut self) -> String {
        let Some(first) = self.consume_ident() else {
            return String::new();
        };
        let mut parts = vec![first];
        while self.cur().kind == TokenKind::Dot {
            self.consume();
            if let Some(part) = self.consume_ident() {
                parts.push(part);
            }
        }
        parts.join(".")
    }

    fn read_scalar_value(&mut self) -> String {
        match self.cur().kind {
            TokenKind::String => {
                let mut parts = Vec::new();
                while self.cur().kind == TokenKind::String {
                    parts.push(self.consume().value);
                }
                parts.join("")
            }
            TokenKind::Number | TokenKind::Ident => self.consume().value,
            TokenKind::Minus => {
                self.consume();
                if self.cur().kind == TokenKind::Number {
                    format!("-{}", self.consume().value)
                } else {
                    "-".to_string()
                }
            }
            _ => {
                self.consume();
                String::new()
            }
        }
    }

    fn skip_to_statement_end(&mut self) {
        while self.cur().kind != TokenKind::Eof {
            match self.cur().kind {
                TokenKind::Semicolon => {
                    self.consume();
                    return;
                }
                TokenKind::LBrace => {
                    self.skip_block();
                    return;
                }
                _ => {
                    self.consume();
                }
            }
        }
    }

    fn skip_block(&mut self) {
        if self.cur().kind != TokenKind::LBrace {
            return;
        }
        self.consume();
        let mut depth = 1usize;
        while self.cur().kind != TokenKind::Eof && depth > 0 {
            match self.cur().kind {
                TokenKind::LBrace => depth += 1,
                TokenKind::RBrace => depth -= 1,
                _ => {}
            }
            self.consume();
        }
    }

    fn skip_bracket_block(&mut self) {
        if self.cur().kind != TokenKind::LBracket {
            return;
        }
        self.consume();
        let mut depth = 1usize;
        while self.cur().kind != TokenKind::Eof && depth > 0 {
            match self.cur().kind {
                TokenKind::LBracket => depth += 1,
                TokenKind::RBracket => depth -= 1,
                _ => {}
            }
            self.consume();
        }
    }

    fn expect(&mut self, kind: TokenKind, message: &str) -> Result<(), ParseError> {
        if self.cur().kind == kind {
            self.consume();
            Ok(())
        } else {
            Err(self.syntax(message))
        }
    }

    fn consume_optional(&mut self, kind: TokenKind) -> bool {
        if self.cur().kind == kind {
            self.consume();
            true
        } else {
            false
        }
    }

    fn consume_ident_if(&mut self, value: &str) -> bool {
        if self.cur().is_ident(value) {
            self.consume();
            true
        } else {
            false
        }
    }

    fn consume_ident(&mut self) -> Option<String> {
        if self.cur().kind == TokenKind::Ident {
            Some(self.consume().value)
        } else {
            None
        }
    }

    fn consume(&mut self) -> Token {
        let token = self.cur().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    fn cur(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .unwrap_or_else(|| self.tokens.last().expect("lexer always emits an EOF token"))
    }

    fn span(&self) -> SourceSpan {
        SourceSpan {
            file: self.file.clone(),
            line: self.cur().line,
            column: self.cur().column,
        }
    }

    fn syntax(&self, message: &str) -> ParseError {
        ParseError::Syntax {
            file: self.file.clone(),
            line: self.cur().line,
            column: self.cur().column,
            message: message.to_string(),
        }
    }
}
