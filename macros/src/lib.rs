use proc_macro::TokenStream;

use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

#[proc_macro_derive(Setters)]
pub fn derive_setters(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let setters = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields
                .named
                .iter()
                .map(|field| {
                    let field_name = field.ident.as_ref().unwrap();
                    let field_type = &field.ty;
                    quote! {
                        pub fn #field_name(mut self, value: #field_type) -> Self {
                            self.#field_name = value;
                            self
                        }
                    }
                })
                .collect::<Vec<_>>(),
            _ => panic!("Setters only supports structs with named fields"),
        },
        _ => panic!("Setters can only be derived for structs"),
    };

    quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            #(#setters)*
        }
    }
    .into()
}
