use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Unknown,
    Ident,
    String,
    Number,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Equal,
    Colon,
    Semicolon,
    Comma,
    Dot,
    Slash,
    Minus,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub value: String,
    pub line: usize,
    pub column: usize,
}

impl Token {
    pub fn is_ident(&self, value: &str) -> bool {
        self.kind == TokenKind::Ident && self.value == value
    }
}

#[derive(Debug, Clone)]
pub struct LexError {
    pub file: String,
    pub line: usize,
    pub column: usize,
    pub message: String,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}: {}",
            self.file, self.line, self.column, self.message
        )
    }
}

impl std::error::Error for LexError {}

pub struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    line: usize,
    col: usize,
    file: String,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a [u8], file: impl Into<String>) -> Self {
        Self {
            src,
            pos: 0,
            line: 1,
            col: 1,
            file: file.into(),
        }
    }

    pub fn tokenize(mut self) -> Result<Vec<Token>, LexError> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            let done = token.kind == TokenKind::Eof;
            tokens.push(token);
            if done {
                return Ok(tokens);
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if is_ws(ch) {
                self.advance();
                continue;
            }
            if ch == b'/' && self.peek(1) == b'/' {
                self.skip_line_comment();
                continue;
            }
            if ch == b'/' && self.peek(1) == b'*' {
                self.skip_block_comment()?;
                continue;
            }
            if ch == b'"' || ch == b'\'' {
                return self.read_string(ch);
            }
            if is_digit(ch) || (ch == b'-' && is_digit(self.peek(1))) {
                return Ok(self.read_number());
            }
            if is_ident_start(ch) {
                return Ok(self.read_ident());
            }

            let line = self.line;
            let column = self.col;
            self.advance();
            let kind = match ch {
                b'{' => TokenKind::LBrace,
                b'}' => TokenKind::RBrace,
                b'[' => TokenKind::LBracket,
                b']' => TokenKind::RBracket,
                b'(' => TokenKind::LParen,
                b')' => TokenKind::RParen,
                b'=' => TokenKind::Equal,
                b':' => TokenKind::Colon,
                b';' => TokenKind::Semicolon,
                b',' => TokenKind::Comma,
                b'.' => TokenKind::Dot,
                b'/' => TokenKind::Slash,
                b'-' => TokenKind::Minus,
                b'<' => {
                    self.skip_angle_block();
                    continue;
                }
                b'>' => continue,
                _ => TokenKind::Unknown,
            };
            return Ok(Token {
                kind,
                value: (ch as char).to_string(),
                line,
                column,
            });
        }

        Ok(Token {
            kind: TokenKind::Eof,
            value: String::new(),
            line: self.line,
            column: self.col,
        })
    }

    fn read_ident(&mut self) -> Token {
        let line = self.line;
        let column = self.col;
        let start = self.pos;
        while self.pos < self.src.len() && is_ident_continue(self.src[self.pos]) {
            self.advance();
        }
        Token {
            kind: TokenKind::Ident,
            value: String::from_utf8_lossy(&self.src[start..self.pos]).to_string(),
            line,
            column,
        }
    }

    fn read_number(&mut self) -> Token {
        let line = self.line;
        let column = self.col;
        let start = self.pos;
        if self.src[self.pos] == b'-' {
            self.advance();
        }
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if is_digit(ch) || ch == b'.' || ch == b'e' || ch == b'E' {
                self.advance();
            } else {
                break;
            }
        }
        Token {
            kind: TokenKind::Number,
            value: String::from_utf8_lossy(&self.src[start..self.pos]).to_string(),
            line,
            column,
        }
    }

    fn read_string(&mut self, quote: u8) -> Result<Token, LexError> {
        let line = self.line;
        let column = self.col;
        self.advance();
        let mut out = Vec::new();
        while self.pos < self.src.len() {
            let ch = self.src[self.pos];
            if ch == b'\\' && self.pos + 1 < self.src.len() {
                self.advance();
                let escaped = self.src[self.pos];
                match escaped {
                    b'n' => out.push(b'\n'),
                    b't' => out.push(b'\t'),
                    b'r' => out.push(b'\r'),
                    b'"' => out.push(b'"'),
                    b'\'' => out.push(b'\''),
                    b'\\' => out.push(b'\\'),
                    other => {
                        out.push(b'\\');
                        out.push(other);
                    }
                }
                self.advance();
                continue;
            }
            if ch == quote {
                self.advance();
                return Ok(Token {
                    kind: TokenKind::String,
                    value: String::from_utf8_lossy(&out).to_string(),
                    line,
                    column,
                });
            }
            out.push(ch);
            self.advance();
        }
        Err(self.error_at(line, column, "unterminated string literal"))
    }

    fn skip_line_comment(&mut self) {
        while self.pos < self.src.len() && self.src[self.pos] != b'\n' {
            self.advance();
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), LexError> {
        let line = self.line;
        let column = self.col;
        self.advance();
        self.advance();
        while self.pos + 1 < self.src.len() {
            if self.src[self.pos] == b'*' && self.src[self.pos + 1] == b'/' {
                self.advance();
                self.advance();
                return Ok(());
            }
            self.advance();
        }
        Err(self.error_at(line, column, "unterminated block comment"))
    }

    fn skip_angle_block(&mut self) {
        let mut depth = 1usize;
        while self.pos < self.src.len() && depth > 0 {
            match self.src[self.pos] {
                b'<' => depth += 1,
                b'>' => depth -= 1,
                _ => {}
            }
            self.advance();
        }
    }

    fn advance(&mut self) {
        if self.pos >= self.src.len() {
            return;
        }
        if self.src[self.pos] == b'\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        self.pos += 1;
    }

    fn peek(&self, offset: usize) -> u8 {
        self.src.get(self.pos + offset).copied().unwrap_or_default()
    }

    fn error_at(&self, line: usize, column: usize, message: &str) -> LexError {
        LexError {
            file: self.file.clone(),
            line,
            column,
            message: message.to_string(),
        }
    }
}

fn is_ws(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\r' | b'\n')
}

fn is_digit(ch: u8) -> bool {
    ch.is_ascii_digit()
}

fn is_ident_start(ch: u8) -> bool {
    ch == b'_' || ch.is_ascii_alphabetic()
}

fn is_ident_continue(ch: u8) -> bool {
    is_ident_start(ch) || is_digit(ch)
}
