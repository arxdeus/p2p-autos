//! Untrusted-input sanitization: the filename and MIME type an uploader's
//! tab supplies. Both reach an HTTP header, so both are a trust boundary.

const MAX_NAME: usize = 255;

/// Strip path separators and control characters — the name reaches a header
/// and a client's filesystem, so it is a trust boundary.
pub(crate) fn sanitize_name(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_NAME)
        .collect();
    match cleaned.trim().trim_matches('.') {
        "" => "download".into(),
        s => s.into(),
    }
}

/// Only a bare `type/subtype` survives; anything else becomes a safe default.
/// A reflected `Content-Type` is an XSS vector, so no parameters are kept.
pub(crate) fn sanitize_mime(raw: &str) -> String {
    let ok = |c: char| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '!' | '#'..='\'' | '*' | '+' | '-' | '.' | '^' | '_' | '`' | '|' | '~'
            )
    };
    let mut parts = raw.trim().split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(t), Some(s), None)
            if !t.is_empty()
                && !s.is_empty()
                && t.len() + s.len() < 128
                && t.chars().all(ok)
                && s.chars().all(ok) =>
        {
            format!("{}/{}", t.to_ascii_lowercase(), s.to_ascii_lowercase())
        }
        _ => "application/octet-stream".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(sanitize_name("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_name("C:\\Users\\a\\b.txt"), "b.txt");
        assert_eq!(sanitize_name("ok\r\nX-Evil: 1"), "okX-Evil: 1");
        assert_eq!(sanitize_name("   "), "download");
        assert_eq!(sanitize_name(".."), "download");
        assert_eq!(sanitize_name("hé.txt"), "hé.txt");
    }

    #[test]
    fn mimes() {
        assert_eq!(sanitize_mime("video/MP4"), "video/mp4");
        assert_eq!(sanitize_mime(""), "application/octet-stream");
        assert_eq!(
            sanitize_mime("text/html; charset=x"),
            "application/octet-stream"
        );
        assert_eq!(sanitize_mime("a/b/c"), "application/octet-stream");
        assert_eq!(sanitize_mime("te xt/plain"), "application/octet-stream");
    }
}
