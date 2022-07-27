Rust tuples only work up to 12 elements, but we may need arbitrarily many.
This library has a set of macros to define such tuples.

#[derive(Copy, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct tuple0;

impl Debug for tuple0 {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_tuple("").finish()
    }
}

impl From<()> for tuple0 {
    fn from(_: ()) -> Self {
        Self
    }
}

impl Into<()> for tuple0 {
    fn into(self) {}
}

macro_rules! declare_tuples {
    (
        $(
            $tuple_name:ident<$($element:tt),* $(,)?>
        ),*
        $(,)?
    ) => {
        $(
            #[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
            pub struct $tuple_name<$($element,)*>($(pub $element,)*);

            impl<$($element),*> From<($($element,)*)> for $tuple_name<$($element,)*> {
                fn from(($($element,)*): ($($element,)*)) -> Self {
                    Self($($element),*)
                }
            }

            impl<$($element),*> Into<($($element,)*)> for $tuple_name<$($element,)*> {
                fn into(self) -> ($($element,)*) {
                    let $tuple_name($($element),*) = self;
                    ($($element,)*)
                }
            }

            impl<$($element: Debug),*> Debug for $tuple_name<$($element),*> {
                fn fmt(&self, f: &mut Formatter) -> FmtResult {
                    let $tuple_name($($element),*) = self;
                    f.debug_tuple("")
                        $(.field(&$element))*
                        .finish()
                }
            }

            impl<$($element: Copy),*> Copy for $tuple_name<$($element),*> {}
        )*
    };
}
