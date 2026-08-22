//! Attribute macro `#[persistent]` — injects `id`, generates `new()`, typed query
//! builders, replace/upsert helpers, and implements the trait.
//!
//! # Architecture patterns for large codebases
//!
//! `#[persistent]` is designed to be used sparingly — only on **aggregates** and
//! **domain entities** that represent state worth saving. Child structs nested
//! inside a persistent root should be plain `Serialize + Deserialize` values.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{
    DeriveInput, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// Marks a struct as persistable.
///
/// Injects an `id: AirId<Self>` field, generates a `new(...)` constructor, and
/// implements `Persistent`. If `Serialize` or `Deserialize` are not already
/// present in a `#[derive(...)]` attribute, they are auto-injected.
///
/// # Example
///
/// ```ignore
/// #[persistent]
/// pub struct ChatSession {
///     pub messages: Vec<Message>,
/// }
/// ```
///
/// # Index columns
///
/// ```ignore
/// #[persistent(index(status, priority))]
/// pub struct Job {
///     pub status: String,
///     pub priority: i32,
/// }
/// ```
///
/// # JSON columns
///
/// ```ignore
/// #[persistent(index(session_uuid))]
/// pub struct StoredMessage {
///     pub session_uuid: String,
///
///     #[stored(json)]
///     pub content: Message,
/// }
/// ```
/// The `content` field is automatically JSON-serialized and stored in a
/// `content_json` TEXT column.
#[proc_macro_attribute]
pub fn persistent(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = if args.is_empty() {
        PersistentArgs { indexes: vec![] }
    } else {
        parse_macro_input!(args as PersistentArgs)
    };
    let input = parse_macro_input!(input as DeriveInput);
    expand_persistent(&args, &input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

fn has_serde_derives(attrs: &[syn::Attribute]) -> (bool, bool) {
    let mut has_serialize = false;
    let mut has_deserialize = false;

    for attr in attrs {
        if attr.path().is_ident("derive") {
            let Ok(paths) = attr.parse_args_with(
                syn::punctuated::Punctuated::<syn::Path, syn::Token![,]>::parse_terminated,
            ) else {
                continue;
            };
            for path in paths {
                let segs: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
                match segs.as_slice() {
                    [s] if s == "Serialize" => has_serialize = true,
                    [s] if s == "Deserialize" => has_deserialize = true,
                    [a, b] if a == "serde" && b == "Serialize" => has_serialize = true,
                    [a, b] if a == "serde" && b == "Deserialize" => has_deserialize = true,
                    _ => {}
                }
            }
        }
    }
    (has_serialize, has_deserialize)
}

fn is_stored_json(attr: &syn::Attribute) -> bool {
    if !attr.path().is_ident("stored") {
        return false;
    }
    if let syn::Meta::List(list) = &attr.meta {
        return list.parse_args::<syn::Ident>().is_ok_and(|i| i == "json");
    }
    false
}

fn validate_struct<'a>(
    ident: &syn::Ident,
    data: &'a syn::Data,
    indexes: &[String],
) -> syn::Result<&'a syn::FieldsNamed> {
    let named = match data {
        syn::Data::Struct(s) => match &s.fields {
            syn::Fields::Named(n) => n,
            _ => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "`#[persistent]` only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "`#[persistent]` only supports structs",
            ));
        }
    };

    if named
        .named
        .iter()
        .any(|f| f.ident.as_ref().is_some_and(|i| i == "id"))
    {
        return Err(syn::Error::new_spanned(
            ident,
            "`#[persistent]` manages its own `id` field — remove the manual `id` field",
        ));
    }

    for idx in indexes {
        let idx_ident = syn::Ident::new(idx, Span::call_site());
        if !named
            .named
            .iter()
            .any(|f| f.ident.as_ref().is_some_and(|i| i == &idx_ident))
        {
            return Err(syn::Error::new_spanned(
                ident,
                format!("index field `{idx}` not found in `{ident}`"),
            ));
        }
    }

    Ok(named)
}

struct ProcessedFields {
    regular_fields: Vec<proc_macro2::TokenStream>,
    index_field_idents: Vec<syn::Ident>,
    index_column_names: Vec<String>,
    all_column_names: Vec<String>,
    all_value_exprs: Vec<proc_macro2::TokenStream>,
}

fn process_fields(indexes: &[String], named: &syn::FieldsNamed) -> ProcessedFields {
    let mut regular_fields = Vec::new();
    let mut json_field_idents = Vec::new();
    let mut index_field_idents = Vec::new();
    let mut index_column_names = Vec::new();

    for field in &named.named {
        let field_ident = field.ident.as_ref().expect("named field has ident");
        let field_ty = &field.ty;
        let mut new_attrs = Vec::new();
        let mut is_json = false;

        for attr in &field.attrs {
            if is_stored_json(attr) {
                is_json = true;
            } else {
                new_attrs.push(attr.clone());
            }
        }

        if is_json {
            new_attrs.push(syn::parse_quote! {
                #[serde(serialize_with = "::airnest::json_ser", deserialize_with = "::airnest::json_de")]
            });
            json_field_idents.push(field_ident.clone());
        }

        let is_index = indexes.iter().any(|i| i == &field_ident.to_string());

        if !is_json && is_index {
            index_field_idents.push(field_ident.clone());
            index_column_names.push(field_ident.to_string());
        }

        regular_fields.push(quote! {
            #(#new_attrs)*
            pub #field_ident: #field_ty,
        });
    }

    let mut all_column_names = index_column_names.clone();
    let mut all_value_exprs: Vec<proc_macro2::TokenStream> = index_field_idents
        .iter()
        .map(|ident| {
            quote! {
                ::airnest::ToIndexValue::to_index_value(&self.#ident)
            }
        })
        .collect();

    for json_ident in &json_field_idents {
        let col_name = format!("{json_ident}_json");
        all_column_names.push(col_name);
        all_value_exprs.push(quote! {
            ::airnest::json_string(&self.#json_ident)
        });
    }

    ProcessedFields {
        regular_fields,
        index_field_idents,
        index_column_names,
        all_column_names,
        all_value_exprs,
    }
}

struct TypedBuilders {
    query_struct_name: syn::Ident,
    query_methods: Vec<proc_macro2::TokenStream>,
    replace_struct_name: syn::Ident,
    replace_methods: Vec<proc_macro2::TokenStream>,
    upsert_struct_name: syn::Ident,
    upsert_methods: Vec<proc_macro2::TokenStream>,
    count_by_methods: Vec<proc_macro2::TokenStream>,
}

fn build_typed_builders(
    ident: &syn::Ident,
    index_field_idents: &[syn::Ident],
    index_column_names: &[String],
) -> TypedBuilders {
    let query_struct_name = syn::Ident::new(&format!("{ident}Query"), Span::call_site());
    let query_methods: Vec<_> = index_field_idents
        .iter()
        .zip(index_column_names)
        .map(|(ident, col_name)| {
            quote! {
                pub fn #ident<V: ::airnest::ToIndexValue + ?Sized>(self, value: &V) -> Self {
                    Self { query: self.query.eq(#col_name, value) }
                }
            }
        })
        .collect();

    let replace_struct_name = syn::Ident::new(&format!("{ident}ReplaceBuilder"), Span::call_site());
    let replace_methods: Vec<_> = index_field_idents
        .iter()
        .zip(index_column_names)
        .map(|(ident, col_name)| {
            quote! {
                pub fn #ident<V: ::airnest::ToIndexValue + ?Sized>(self, value: &V) -> Self {
                    Self(self.0.eq(#col_name, value))
                }
            }
        })
        .collect();

    let upsert_struct_name = syn::Ident::new(&format!("{ident}UpsertBuilder"), Span::call_site());
    let upsert_methods: Vec<_> = index_field_idents
        .iter()
        .zip(index_column_names)
        .map(|(ident, col_name)| {
            quote! {
                pub fn #ident<V: ::airnest::ToIndexValue + ?Sized>(self, value: &V) -> Self {
                    Self(self.0.eq(#col_name, value))
                }
            }
        })
        .collect();

    let count_by_methods: Vec<_> = index_field_idents
        .iter()
        .zip(index_column_names)
        .map(|(field_ident, col_name)| {
            let method_name = syn::Ident::new(&format!("count_by_{field_ident}"), Span::call_site());
            quote! {
                pub async fn #method_name(store: &::airnest::Store) -> ::std::result::Result<::std::collections::HashMap<::std::string::String, i64>, ::airnest::StoreError> {
                    store.count_grouped_by::<#ident>(#col_name).await
                }
            }
        })
        .collect();

    TypedBuilders {
        query_struct_name,
        query_methods,
        replace_struct_name,
        replace_methods,
        upsert_struct_name,
        upsert_methods,
        count_by_methods,
    }
}

fn expand_persistent(
    args: &PersistentArgs,
    input: &DeriveInput,
) -> syn::Result<proc_macro2::TokenStream> {
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let vis = &input.vis;
    let table_name = ident.to_string();

    let named = validate_struct(ident, &input.data, &args.indexes)?;
    let pf = process_fields(&args.indexes, named);

    // Pull the pieces we splice into the final quote! out of `pf` so we can
    // use them in repetition patterns (`#(#var)*`).
    let regular_fields = pf.regular_fields;
    let index_field_idents = pf.index_field_idents;
    let index_column_names = pf.index_column_names;
    let all_column_names = pf.all_column_names;
    let all_value_exprs = pf.all_value_exprs;

    // ── constructor fields ────────────────────────────────────────────────────
    let field_names: Vec<&syn::Ident> = named
        .named
        .iter()
        .map(|f| f.ident.as_ref().expect("named field has ident"))
        .collect();
    let field_types: Vec<&syn::Type> = named.named.iter().map(|f| &f.ty).collect();

    // ── auto-inject missing serde derives ─────────────────────────────────────
    let (has_serialize, has_deserialize) = has_serde_derives(&input.attrs);
    let extra_derive = match (has_serialize, has_deserialize) {
        (false, false) => Some(quote! { #[derive(::serde::Serialize, ::serde::Deserialize)] }),
        (false, true) => Some(quote! { #[derive(::serde::Serialize)] }),
        (true, false) => Some(quote! { #[derive(::serde::Deserialize)] }),
        (true, true) => None,
    };

    let TypedBuilders {
        query_struct_name,
        query_methods,
        replace_struct_name,
        replace_methods,
        upsert_struct_name,
        upsert_methods,
        count_by_methods,
    } = build_typed_builders(ident, &index_field_idents, &index_column_names);

    let attrs = &input.attrs;

    Ok(render_output(&RenderArgs {
        attrs,
        extra_derive: extra_derive.as_ref(),
        vis,
        ident,
        impl_generics: &impl_generics,
        ty_generics: &ty_generics,
        where_clause,
        regular_fields: &regular_fields,
        field_names: &field_names,
        field_types: &field_types,
        table_name: &table_name,
        query_struct_name: &query_struct_name,
        query_methods: &query_methods,
        replace_struct_name: &replace_struct_name,
        replace_methods: &replace_methods,
        upsert_struct_name: &upsert_struct_name,
        upsert_methods: &upsert_methods,
        count_by_methods: &count_by_methods,
        all_column_names: &all_column_names,
        all_value_exprs: &all_value_exprs,
    }))
}

struct RenderArgs<'a> {
    attrs: &'a [syn::Attribute],
    extra_derive: Option<&'a proc_macro2::TokenStream>,
    vis: &'a syn::Visibility,
    ident: &'a syn::Ident,
    impl_generics: &'a syn::ImplGenerics<'a>,
    ty_generics: &'a syn::TypeGenerics<'a>,
    where_clause: Option<&'a syn::WhereClause>,
    regular_fields: &'a [proc_macro2::TokenStream],
    field_names: &'a [&'a syn::Ident],
    field_types: &'a [&'a syn::Type],
    table_name: &'a str,
    query_struct_name: &'a syn::Ident,
    query_methods: &'a [proc_macro2::TokenStream],
    replace_struct_name: &'a syn::Ident,
    replace_methods: &'a [proc_macro2::TokenStream],
    upsert_struct_name: &'a syn::Ident,
    upsert_methods: &'a [proc_macro2::TokenStream],
    count_by_methods: &'a [proc_macro2::TokenStream],
    all_column_names: &'a [String],
    all_value_exprs: &'a [proc_macro2::TokenStream],
}

fn render_output(r: &RenderArgs<'_>) -> proc_macro2::TokenStream {
    let RenderArgs {
        attrs,
        vis,
        ident,
        impl_generics,
        ty_generics,
        where_clause,
        regular_fields,
        field_names,
        field_types,
        table_name,
        query_struct_name,
        query_methods,
        replace_struct_name,
        replace_methods,
        upsert_struct_name,
        upsert_methods,
        count_by_methods,
        all_column_names,
        all_value_exprs,
        extra_derive,
    } = r;
    quote! {
        #(#attrs)*
        #extra_derive
        #vis struct #ident #impl_generics #where_clause {
            /// Auto-generated UUIDv7 id. Managed by `#[persistent]`.
            pub id: ::airnest::AirId<#ident>,
            #(#regular_fields)*
        }

        impl #impl_generics #ident #ty_generics #where_clause {
            /// Create a new instance with a fresh UUIDv7 id.
            pub fn new(#(#field_names: #field_types),*) -> Self {
                Self {
                    id: ::airnest::AirId::new(),
                    #(#field_names),*
                }
            }

            /// Return the auto-generated id.
            pub fn id(&self) -> ::airnest::AirId<Self> {
                self.id
            }

            /// Start a typed query for this type.
            pub fn find(store: &::airnest::Store) -> #query_struct_name<'_> {
                #query_struct_name { query: store.find::<#ident>() }
            }

            /// Start a replace operation for this type.
            pub fn replace_for(store: &::airnest::Store) -> #replace_struct_name<'_> {
                #replace_struct_name::new(store)
            }

            /// Start an upsert operation for this type.
            pub fn upsert(store: &::airnest::Store) -> #upsert_struct_name<'_> {
                #upsert_struct_name::new(store)
            }

            #(#count_by_methods)*
        }

        #vis struct #query_struct_name<'a> {
            query: ::airnest::Query<'a, #ident>,
        }

        impl<'a> #query_struct_name<'a> {
            #(#query_methods)*

            pub fn order_by(self, column: &str, order: ::airnest::Order) -> Self {
                Self { query: self.query.order_by(column, order) }
            }

            pub fn limit(self, n: usize) -> Self {
                Self { query: self.query.limit(n) }
            }

            pub async fn all(self) -> ::std::result::Result<::std::vec::Vec<#ident>, ::airnest::StoreError> {
                self.query.all().await
            }

            pub async fn first(self) -> ::std::result::Result<::std::option::Option<#ident>, ::airnest::StoreError> {
                self.query.first().await
            }

            pub async fn count(self) -> ::std::result::Result<i64, ::airnest::StoreError> {
                self.query.count().await
            }
        }

        #vis struct #replace_struct_name<'a>(::airnest::ReplaceBuilder<'a, #ident>);

        impl<'a> #replace_struct_name<'a> {
            fn new(store: &'a ::airnest::Store) -> Self {
                Self(::airnest::ReplaceBuilder::new(store))
            }

            #(#replace_methods)*

            pub async fn items(self, items: ::std::vec::Vec<#ident>) -> ::std::result::Result<(), ::airnest::StoreError> {
                self.0.items(items).await
            }
        }

        #vis struct #upsert_struct_name<'a>(::airnest::UpsertBuilder<'a, #ident>);

        impl<'a> #upsert_struct_name<'a> {
            fn new(store: &'a ::airnest::Store) -> Self {
                Self(::airnest::UpsertBuilder::new(store))
            }

            #(#upsert_methods)*

            pub fn modify<F: FnOnce(&mut #ident)>(self, f: F) -> ::airnest::UpsertModifyBuilder<'a, #ident, F> {
                self.0.modify(f)
            }
        }

        impl #impl_generics ::airnest::Persistent for #ident #ty_generics #where_clause {
            fn id(&self) -> ::airnest::AirId<Self> {
                self.id
            }

            const TABLE: &'static str = #table_name;

            fn index_columns() -> &'static [&'static str] {
                &[#(#all_column_names),*]
            }

            fn index_values(&self) -> ::std::vec::Vec<::std::string::String> {
                ::std::vec![#(#all_value_exprs),*]
            }
        }
    }
}

// ── attribute parser ──────────────────────────────────────────────────────────

/// Parsed form of `#[persistent(index(a, b, c))]` with legacy `key = ...` support.
struct PersistentArgs {
    indexes: Vec<String>,
}

impl Parse for PersistentArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Support legacy `key = <ident>` (ignored — new `id` is the PK) and `index(...)`.
        let mut indexes = Vec::new();
        while !input.is_empty() {
            if input.peek(syn::Ident) {
                let lookahead = input.fork();
                let ident: syn::Ident = lookahead.parse().unwrap();
                if ident == "key" {
                    let _: syn::Ident = input.parse()?;
                    input.parse::<Token![=]>()?;
                    let _: syn::Ident = input.parse()?;
                    if input.peek(Token![,]) {
                        input.parse::<Token![,]>()?;
                    }
                    continue;
                }
            }
            let kw: syn::Ident = input.parse()?;
            if kw != "index" {
                return Err(syn::Error::new_spanned(
                    kw,
                    "expected `index(field, ...)`. Bare `#[persistent]` needs no arguments.",
                ));
            }
            let content;
            syn::parenthesized!(content in input);
            while !content.is_empty() {
                let ident: syn::Ident = content.parse()?;
                indexes.push(ident.to_string());
                if content.peek(Token![,]) {
                    content.parse::<Token![,]>()?;
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self { indexes })
    }
}
