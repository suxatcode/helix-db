//! Semantic analyzer for Helix‑QL.

use colored::Colorize;

use crate::helixc::parser::{helix_parser::*, location::Loc};

use std::collections::{HashMap, HashSet};

use super::pretty;

/// A single diagnostic to be surfaced to the editor.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub location: Loc,
    pub message: String,
    pub hint: Option<String>,
    pub filename: Option<String>,
    pub severity: DiagnosticSeverity,
}

#[derive(Debug, Clone)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
    Empty,
}

impl Diagnostic {
    pub fn new(
        location: Loc,
        message: impl Into<String>,
        severity: DiagnosticSeverity,
        hint: Option<String>,
    ) -> Self {
        Self {
            location,
            message: message.into(),
            hint,
            filename: None,
            severity,
        }
    }

    pub fn render(&self, src: &str, filename: &str) -> String {
        pretty::render(self, src, filename)
    }
}

pub fn analyze(src: &Source) -> Vec<Diagnostic> {
    let mut ctx = Ctx::new(src);
    ctx.check_schema();
    ctx.check_queries();
    ctx.diagnostics
}

/// Internal working context shared by all passes.
struct Ctx<'a> {
    src: &'a Source,

    /// Quick look‑ups
    node_set: HashSet<&'a str>,
    vector_set: HashSet<&'a str>,
    edge_map: HashMap<&'a str, &'a EdgeSchema>,
    node_fields: HashMap<&'a str, HashSet<&'a str>>,
    edge_fields: HashMap<&'a str, HashSet<&'a str>>,

    diagnostics: Vec<Diagnostic>,
}

impl<'a> Ctx<'a> {
    fn new(src: &'a Source) -> Self {
        // Build field look‑ups once
        let node_fields = src
            .node_schemas
            .iter()
            .map(|n| {
                (
                    n.name.1.as_str(),
                    n.fields.iter().map(|f| f.name.as_str()).collect(),
                )
            })
            .collect();

        let edge_fields = src
            .edge_schemas
            .iter()
            .map(|e| {
                (
                    e.name.1.as_str(),
                    e.properties
                        .as_ref()
                        .map(|v| v.iter().map(|f| f.name.as_str()).collect())
                        .unwrap_or_else(HashSet::new),
                )
            })
            .collect();

        Self {
            node_set: src.node_schemas.iter().map(|n| n.name.1.as_str()).collect(),
            vector_set: src.vector_schemas.iter().map(|v| v.name.as_str()).collect(),
            edge_map: src
                .edge_schemas
                .iter()
                .map(|e| (e.name.1.as_str(), e))
                .collect(),
            node_fields,
            edge_fields,
            src,
            diagnostics: Vec::new(),
        }
    }

    // ---------- Pass #1: schema --------------------------
    /// Validate that every edge references declared node types.
    fn check_schema(&mut self) {
        for edge in &self.src.edge_schemas {
            if !self.node_set.contains(edge.from.1.as_str()) {
                self.push_schema_err(
                    &edge.from.1,
                    edge.from.0.clone(),
                    format!("`{}` is not a declared node type", edge.from.1),
                    Some(format!("Declare `N::{}` before this edge", edge.from.1)),
                );
            }
            if !self.node_set.contains(edge.to.1.as_str()) {
                self.push_schema_err(
                    &edge.to.1,
                    edge.to.0.clone(),
                    format!("`{}` is not a declared node type", edge.to.1),
                    Some(format!("Declare `N::{}` before this edge", edge.to.1)),
                );
            }
        }
    }

    // ---------- Pass #2: queries -------------------------
    fn check_queries(&mut self) {
        for q in &self.src.queries {
            self.check_query(q);
        }
    }

    fn check_query(&mut self, q: &'a Query) {
        // -------------------------------------------------
        // 2‑a. Parameter validation
        // -------------------------------------------------
        for param in &q.parameters {
            if let FieldType::Identifier(ref id) = param.param_type.1 {
                if !self.node_set.contains(id.as_str()) && !self.vector_set.contains(id.as_str()) {
                    self.push_query_err(
                        q,
                        param.param_type.0.clone(),
                        format!("unknown type `{}` for parameter `{}`", id, param.name.1),
                        "declare or use a matching schema object or use a primitive type",
                    );
                }
            }
        }

        // -------------------------------------------------
        // 2‑b. Statement‑by‑statement walk
        // -------------------------------------------------
        let mut scope: HashMap<&str, Type<'a>> = HashMap::new();
        for param in &q.parameters {
            scope.insert(param.name.1.as_str(), Type::from(&param.param_type.1));
        }

        use StatementType::*;
        for stmt in &q.statements {
            match &stmt.statement {
                Assignment(assign) => {
                    if scope.contains_key(assign.variable.as_str()) {
                        self.push_query_err(
                            q,
                            assign.loc.clone(),
                            format!("variable `{}` is already declared", assign.variable),
                            "rename the new variable or remove the previous definition",
                        );
                        continue;
                    }

                    let rhs_ty = self.infer_expr_type(&assign.value, &scope, q);
                    scope.insert(assign.variable.as_str(), rhs_ty);
                }

                AddNode(add) => {
                    if let Some(ref ty) = add.node_type {
                        if !self.node_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddN<{}>` refers to unknown node type", ty),
                                "declare the node schema first",
                            );
                        }
                    }
                }

                AddEdge(add) => {
                    if let Some(ref ty) = add.edge_type {
                        if !self.edge_map.contains_key(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddE<{}>` refers to unknown edge type", ty),
                                "declare the edge schema first",
                            );
                        }
                    }
                }

                AddVector(add) => {
                    if let Some(ref ty) = add.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                BatchAddVector(add) => {
                    if let Some(ref ty) = add.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                Drop(expr) => {
                    // Nothing special right now; still type‑check sub‑expressions
                    self.infer_expr_type(expr, &scope, q);
                }

                SearchVector(expr) => {
                    if let Some(ref ty) = expr.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                expr.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                ForLoop(fl) => {
                    // Ensure the collection exists
                    if !scope.contains_key(fl.in_variable.as_str()) {
                        self.push_query_err(
                            q,
                            fl.loc.clone(),
                            format!("`{}` is not defined in the current scope", fl.in_variable),
                            "add a statement assigning it before the loop",
                        );
                    }
                    // Add loop vars to new child scope and walk the body
                    let mut body_scope = scope.clone();
                    for v in &fl.variables {
                        body_scope.insert(v.as_str(), Type::Unknown);
                    }
                    for body_stmt in &fl.statements {
                        // Recursive walk (but without infinite nesting for now)
                        if let StatementType::Assignment(a) = &body_stmt.statement {
                            let t = self.infer_expr_type(&a.value, &body_scope, q);
                            body_scope.insert(a.variable.as_str(), t);
                        }
                    }
                }

                _ => { /* SearchVector handled above; others TBD */ }
            }
        }

        // -------------------------------------------------
        // 2‑c. Validate RETURN expressions
        // -------------------------------------------------
        if q.return_values.is_empty() {
            let end = q.loc.end.clone();
            self.push_query_warn(
                q,
                Loc::new(end.clone(), end, q.loc.span.clone()),
                "query has no RETURN clause".to_string(),
                "add `RETURN <expr>` at the end",
            );
        }
        for ret in &q.return_values {
            self.infer_expr_type(ret, &scope, q);
        }
    }

    // -----------------------------------------------------
    // Helpers
    // -----------------------------------------------------
    fn push_schema_err(&mut self, _ident: &str, loc: Loc, msg: String, hint: Option<String>) {
        self.diagnostics
            .push(Diagnostic::new(loc, msg, DiagnosticSeverity::Error, hint));
    }
    fn push_query_err(&mut self, q: &Query, loc: Loc, msg: String, hint: impl Into<String>) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            format!("{} (in QUERY named `{}`)", msg, q.name),
            DiagnosticSeverity::Error,
            Some(hint.into()),
        ));
    }

    fn push_query_warn(&mut self, q: &Query, loc: Loc, msg: String, hint: impl Into<String>) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            format!("{} (in QUERY named `{}`)", msg, q.name),
            DiagnosticSeverity::Warning,
            Some(hint.into()),
        ));
    }

    /// Infer the semantic `Type` of an expression *and* perform all traversal
    ///‑specific validations (property access, field exclusions, step ordering).
    fn infer_expr_type(
        &mut self,
        expression: &'a Expression,
        scope: &HashMap<&'a str, Type<'a>>,
        q: &'a Query,
    ) -> Type<'a> {
        use ExpressionType::*;
        let expr = &expression.expr;
        match expr {
            Identifier(name) => match scope.get(name.as_str()) {
                Some(t) => t.clone(),
                None => {
                    self.push_query_err(
                        q,
                        expression.loc.clone(),
                        format!("identifier `{}` is not in scope", name),
                        "declare it earlier or fix the typo",
                    );
                    Type::Unknown
                }
            },

            IntegerLiteral(_) => Type::Scalar(FieldType::I32),
            FloatLiteral(_) => Type::Scalar(FieldType::F64),
            StringLiteral(_) => Type::Scalar(FieldType::String),
            BooleanLiteral(_) => Type::Boolean,
            Empty => Type::Unknown,

            Traversal(tr) | Exists(tr) => {
                let final_ty = self.check_traversal(tr, scope, q);
                if matches!(expr, Exists(_)) {
                    Type::Boolean
                } else {
                    final_ty
                }
            }

            AddNode(add) => {
                if let Some(ref ty) = add.node_type {
                    if !self.node_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("`AddN<{}>` refers to unknown node type", ty),
                            "declare the node schema first",
                        );
                    }
                }
                Type::Nodes(add.node_type.as_deref())
            }
            AddEdge(add) => {
                if let Some(ref ty) = add.edge_type {
                    if !self.edge_map.contains_key(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("`AddE<{}>` refers to unknown edge type", ty),
                            "declare the edge schema first",
                        );
                    }
                }
                Type::Edges(add.edge_type.as_deref())
            }
            AddVector(add) => {
                if let Some(ref ty) = add.vector_type {
                    if !self.vector_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("vector type `{}` has not been declared", ty),
                            "add a `V::{}` schema first",
                        );
                    }
                }
                Type::Vector(add.vector_type.as_deref())
            }
            BatchAddVector(add) => {
                if let Some(ref ty) = add.vector_type {
                    if !self.vector_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("vector type `{}` has not been declared", ty),
                            "add a `V::{}` schema first",
                        );
                    }
                }
                Type::Vector(add.vector_type.as_deref())
            }
            SearchVector(sv) => {
                if let Some(ref ty) = sv.vector_type {
                    if !self.vector_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            sv.loc.clone(),
                            format!("vector type `{}` has not been declared", ty),
                            "add a `V::{}` schema first",
                        );
                    }
                }
                // Search returns nodes that contain the vectors
                Type::Nodes(None)
            }
            And(v) | Or(v) => {
                for e in v {
                    self.infer_expr_type(e, scope, q);
                }
                Type::Boolean
            }
        }
    }

    // -----------------------------------------------------
    // Traversal‑specific checks
    // -----------------------------------------------------
    fn check_traversal(
        &mut self,
        tr: &'a Traversal,
        scope: &HashMap<&'a str, Type<'a>>,
        q: &'a Query,
    ) -> Type<'a> {
        // 1. Establish the starting type
        let mut cur_ty = match &tr.start {
            StartNode::Node { types, .. } => {
                if let Some(ref ts) = types {
                    // Check node types exist
                    for t in ts {
                        if !self.node_set.contains(t.as_str()) {
                            self.push_query_err(
                                q,
                                tr.loc.clone(),
                                format!("unknown node type `{}`", t),
                                "declare it with `N::` first",
                            );
                        }
                    }
                    Type::Nodes(ts.first().map(|s| s.as_str()))
                } else {
                    Type::Nodes(None)
                }
            }
            StartNode::Edge { types, .. } => {
                if let Some(ref ts) = types {
                    for t in ts {
                        if !self.edge_map.contains_key(t.as_str()) {
                            self.push_query_err(
                                q,
                                tr.loc.clone(),
                                format!("unknown edge type `{}`", t),
                                "declare it with `E::` first",
                            );
                        }
                    }
                    Type::Edges(ts.first().map(|s| s.as_str()))
                } else {
                    Type::Edges(None)
                }
            }
            StartNode::Variable(var) => scope.get(var.as_str()).cloned().unwrap_or_else(|| {
                self.push_query_err(
                    q,
                    tr.loc.clone(),
                    format!("identifier `{}` is not in scope", var),
                    "declare it earlier or fix the typo",
                );
                Type::Unknown
            }),
            StartNode::Anonymous => Type::Unknown,
        };

        // Track excluded fields for property validation
        let mut excluded: HashSet<&str> = HashSet::new();

        // Stream through the steps
        for graph_step in &tr.steps {
            let step = &graph_step.step;
            match step {
                StepType::Node(gs) | StepType::Edge(gs) => {
                    match self.apply_graph_step(&gs, &cur_ty, q) {
                        Some(new_ty) => cur_ty = new_ty,
                        None => { /* error already recorded */ }
                    }
                    excluded.clear(); // Traversal to a new element resets exclusions
                }

                StepType::Count => {
                    cur_ty = Type::Scalar(FieldType::I64);
                    excluded.clear();
                }

                StepType::Exclude(ex) => {
                    for f in &ex.fields {
                        excluded.insert(f.as_str());
                    }
                }

                StepType::Object(obj) => {
                    // Only validate simple `{name}`‑style accesses
                    if let Type::Nodes(Some(node_ty)) = &cur_ty {
                        if let Some(field_set) = self.node_fields.get(node_ty).cloned() {
                            for (key, _val) in &obj.fields {
                                if excluded.contains(key.as_str()) {
                                    self.push_query_err(
                                        q,
                                        tr.loc.clone(),
                                        format!(
                                            "field `{}` was previously excluded in this traversal",
                                            key
                                        ),
                                        "remove it from the exclusion or move the exclusion later",
                                    );
                                } else if !field_set.contains(key.as_str()) {
                                    self.push_query_err(
                                        q,
                                        tr.loc.clone(),
                                        format!("`{}` is not a field of node `{}`", key, node_ty),
                                        "check the schema field names",
                                    );
                                }
                            }
                        }
                    }
                    // The result of an object projection is usually a scalar or JSON value;
                    // we mark it unknown for type‑flow purposes.
                    cur_ty = Type::Unknown;
                }

                StepType::Where(expr) => {
                    self.infer_expr_type(expr, scope, q);
                    // Where/boolean ops don’t change the element type,
                    // so `cur_ty` stays the same.
                }

                StepType::BooleanOperation(expr) => {
                    // self.infer_expr_type(expr, scope, q);
                    // Where/boolean ops don’t change the element type,
                    // so `cur_ty` stays the same.
                }

                StepType::Update(_) => {
                    // Update returns the same type (nodes/edges) it started with.
                    excluded.clear();
                }

                StepType::AddEdge(add) => {
                    if let Some(ref ty) = add.edge_type {
                        if !self.edge_map.contains_key(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddE<{}>` refers to unknown edge type", ty),
                                "declare the edge schema first",
                            );
                        }
                    }
                    cur_ty = Type::Edges(add.edge_type.as_deref());
                    excluded.clear();
                }

                StepType::Range(_) => { /* doesn’t affect type */ }
                StepType::Closure(cl) => {
                    // Add identifier to a temporary scope so inner uses pass
                    // let mut tmp_scope = scope.clone();
                    // tmp_scope.insert(cl.identifier.as_str(), cur_ty.clone());
                    // // Walk the object literal
                    // let obj_expr = Expression::Object(Object {
                    //     fields: cl.object.fields.clone(),
                    //     should_spread: cl.object.should_spread,
                    // });
                    // self.infer_expr_type(&obj_expr, &tmp_scope, q);
                    // cur_ty = Type::Unknown;
                }

                StepType::SearchVector(_) => {
                    // SearchV on a traversal returns nodes again
                    cur_ty = Type::Nodes(None);
                    excluded.clear();
                }
            }
        }

        cur_ty
    }

    /// Check that a graph‑navigation step is allowed for the current element
    /// kind and return the post‑step kind.
    fn apply_graph_step(
        &mut self,
        gs: &'a GraphStep,
        cur_ty: &Type<'a>,
        q: &'a Query,
    ) -> Option<Type<'a>> {
        use GraphStepType::*;
        match (&gs.step, cur_ty) {
            // Node‑to‑Edge
            (OutE(_) | InE(_), Type::Nodes(_)) => {
                Some(Type::Edges(Some(gs.loc.span.trim_matches(|c: char| {
                    c == '"' || c.is_whitespace() || c == '\n'
                }))))
            }

            // Node‑to‑Node
            (Out(_) | In(_), Type::Nodes(_)) => {
                Some(Type::Nodes(Some(gs.loc.span.trim_matches(|c: char| {
                    c == '"' || c.is_whitespace() || c == '\n'
                }))))
            }

            // Edge‑to‑Node
            (FromN | ToN, Type::Edges(_)) => {
                Some(Type::Nodes(Some(gs.loc.span.trim_matches(|c: char| {
                    c == '"' || c.is_whitespace() || c == '\n'
                }))))
            }

            // Anything else is illegal
            _ => {
                self.push_query_err(
                    q,
                    gs.loc.clone(),
                    format!(
                        "traversal step `{}` cannot follow a step that returns {}",
                        gs.loc
                            .span
                            .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                            .bold(),
                        cur_ty
                            .kind_str()
                            .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                            .bold()
                    ),
                    self.get_traversal_step_hint(cur_ty, &gs.step).as_str(),
                );
                None
            }
        }
    }

    fn get_traversal_step_hint(
        &self,
        current_step: &Type<'a>,
        next_step: &GraphStepType,
    ) -> String {
        match (current_step, next_step) {
            (Type::Nodes(Some(span)), GraphStepType::ToN | GraphStepType::FromN) => {
                format!(
                    "\n{}\n{}",
                    format!(
                        "      • Use `OutE` or `InE` to traverse edges from `{}`",
                        span
                    ),
                    format!(
                        "      • Use `Out` or `In` to traverse nodes from `{}`",
                        span
                    ),
                )
            }
            (Type::Edges(Some(span)), GraphStepType::OutE(_) | GraphStepType::InE(_)) => {
                format!("use `FromN` or `ToN` to traverse nodes from `{}`", span)
            }
            (Type::Edges(Some(span)), GraphStepType::Out(_) | GraphStepType::In(_)) => {
                format!("use `FromN` or `ToN` to traverse nodes from `{}`", span)
            }
            (_, _) => "re-order the traversal or remove the invalid step".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
enum Type<'a> {
    Nodes(Option<&'a str>),
    Edges(Option<&'a str>),
    Vector(Option<&'a str>),
    Scalar(FieldType),
    Boolean,
    Unknown,
}

impl<'a> Type<'a> {
    fn kind_str(&self) -> &'static str {
        match self {
            Type::Nodes(_) => "nodes",
            Type::Edges(_) => "edges",
            Type::Vector(_) => "vectors",
            Type::Scalar(_) => "scalar",
            Type::Boolean => "boolean",
            Type::Unknown => "unknown",
        }
    }
}

impl<'a> From<&'a FieldType> for Type<'a> {
    fn from(ft: &'a FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128 => {
                Type::Scalar(ft.clone())
            }
            Array(_) | Object(_) | Identifier(_) => Type::Unknown,
        }
    }
}

// ---------------------------------
// Tests
// ---------------------------------
#[cfg(test)]
mod analyzer_tests {
    use super::*;
    use crate::helixc::parser::helix_parser::HelixParser;

    /// Convenience helper – parse text and return diagnostics.
    fn run(src: &str) -> Vec<Diagnostic> {
        let parsed = HelixParser::parse_source(src)
            .expect("parser should succeed – these tests are for the analyzer");
        analyze(&parsed)
    }

    #[test]
    fn reports_unknown_node_in_edge() {
        let hx = r#"
            E::Likes {
                From: User,
                To: Post,
                Properties: {}
            }
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("not a declared node type")),
            "expected a diagnostic about undeclared node types, got: {:?}",
            diags
        );
    }

    #[test]
    fn detects_redeclared_variable() {
        let hx = r#"
            N::User { Name: String }

            QUERY dupVar() =>
                u <- N<User>
                u <- N<User>
                RETURN u
        "#;
        let diags = run(hx);
        assert!(
            diags.iter().any(|d| d.message.contains("already declared")),
            "expected a diagnostic about variable redeclaration, got: {:?}",
            diags
        );
    }

    #[test]
    fn flags_invalid_property_access() {
        let hx = r#"
            N::User { name: String }

            QUERY badField() =>
                u <- N<User>
                n <- u::{age}
                RETURN n
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of node")),
            "expected a diagnostic about invalid property access, got: {:?}",
            diags
        );
    }

    #[test]
    fn traversal_step_order_enforced() {
        let hx = r#"
            N::User { name: String }

            QUERY wrongStep() =>
                e <- N<User>::FromN   // OutN on nodes is illegal (needs Edge)
                RETURN e
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("cannot follow a step that returns")),
            "expected a diagnostic about illegal step ordering, got: {:?}",
            diags
        );
    }

    #[test]
    fn clean_query_produces_no_diagnostics() {
        let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {}
            }

            QUERY ok(hey: Usr) =>
                u <- N<User>
                p <- u::Out<Wrote>
                RETURN p::{title}
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(
            diags.is_empty(),
            "expected no diagnostics, got: {:?}",
            diags
        );
    }
}
