/// Shorthand for `Expr::col(name)` when building sea_query expressions.
/// Named `expr_col!` to avoid collision with sqlx's `column!` macro.
#[macro_export]
macro_rules! columnn {
    ($name:expr) => {
        ::sea_query::Expr::col($name)
    };
}
