//! The functions an expression may call, and nothing else.
//!
//! Every function is listed here by name. DataFusion's own registry is never
//! consulted, so `now()`, `random()`, `uuid()` and anything that reads the
//! clock, the environment or the session time zone can't be reached
//! (purity, see the module docs). A test checks that every listed function
//! is `Volatility::Immutable`.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, ArrayRef, ListArray, StructArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue, TableReference, exec_err, plan_err};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, HigherOrderUDF, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TableSource, Volatility, WindowUDF,
};

use super::convert::{self, Target};

/// Function names users may write, lowercase. SQL syntax that DataFusion
/// plans as a function (`TRIM(...)`, `SUBSTRING(... FROM ...)`,
/// `POSITION(... IN ...)`, `CEIL(...)`) reaches these names too.
static USER: LazyLock<BTreeMap<&'static str, Arc<ScalarUDF>>> = LazyLock::new(|| {
    use datafusion_functions::{core, math, string, unicode};
    BTreeMap::from([
        // dataferry's strict conversions (see `convert`).
        ("int", ferry_cast(Target::Int)),
        ("float", ferry_cast(Target::Float)),
        ("string", ferry_cast(Target::String)),
        // Nulls and choices.
        ("coalesce", core::coalesce()),
        ("nullif", core::nullif()),
        ("greatest", core::greatest()),
        ("least", core::least()),
        // Numbers. `round` is half away from zero on decimals.
        ("abs", math::abs()),
        ("ceil", math::ceil()),
        ("floor", math::floor()),
        ("round", math::round()),
        ("trunc", math::trunc()),
        ("power", math::power()),
        ("signum", math::signum()),
        // Text.
        ("lower", string::lower()),
        ("upper", string::upper()),
        ("trim", string::btrim()),
        ("btrim", string::btrim()),
        ("ltrim", string::ltrim()),
        ("rtrim", string::rtrim()),
        ("concat", string::concat()),
        ("concat_ws", string::concat_ws()),
        ("replace", string::replace()),
        ("starts_with", string::starts_with()),
        ("ends_with", string::ends_with()),
        ("split_part", string::split_part()),
        ("contains", string::contains()),
        ("repeat", string::repeat()),
        ("octet_length", string::octet_length()),
        ("length", unicode::character_length()),
        ("char_length", unicode::character_length()),
        ("character_length", unicode::character_length()),
        ("substr", unicode::substr()),
        ("substring", unicode::substring()),
        ("left", unicode::left()),
        ("right", unicode::right()),
        ("lpad", unicode::lpad()),
        ("rpad", unicode::rpad()),
        ("strpos", unicode::strpos()),
        ("reverse", unicode::reverse()),
        ("translate", unicode::translate()),
        // Arrays.
        (
            "cardinality",
            datafusion_functions_nested::cardinality::cardinality_udf(),
        ),
    ])
});

/// Functions only the resolver emits. Users can't name them: the resolver
/// rejects identifiers that start with `__ferry`.
pub(crate) const GET_FIELD: &str = "__ferry_get_field";
pub(crate) const LIST_FIELD: &str = "__ferry_list_field";
pub(crate) const ARRAY_HAS: &str = "__ferry_array_has";

static INTERNAL: LazyLock<BTreeMap<&'static str, Arc<ScalarUDF>>> = LazyLock::new(|| {
    BTreeMap::from([
        (GET_FIELD, datafusion_functions::core::get_field()),
        (LIST_FIELD, Arc::new(ScalarUDF::from(ListField::new()))),
        (
            ARRAY_HAS,
            datafusion_functions_nested::array_has::array_has_udf(),
        ),
    ])
});

/// Whether users may call `name` (lowercase).
pub(crate) fn is_user_function(name: &str) -> bool {
    USER.contains_key(name)
}

/// Every name users may call, sorted, for "did you mean" and docs.
pub fn names() -> impl Iterator<Item = &'static str> {
    USER.keys().copied()
}

fn ferry_cast(target: Target) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(FerryCast::new(target)))
}

/// The [`ContextProvider`] the SQL planner sees: the functions above, no
/// tables, no variables, fixed options.
pub(crate) struct Functions {
    options: ConfigOptions,
    /// Plans `SUBSTRING(x FROM a FOR b)` (as `substr`, which is listed).
    planners: Vec<Arc<dyn ExprPlanner>>,
}

impl Functions {
    pub(crate) fn new() -> Self {
        // Built here, never read from the environment or a session.
        Self {
            options: ConfigOptions::default(),
            planners: vec![Arc::new(
                datafusion_functions::unicode::planner::UnicodeFunctionPlanner,
            )],
        }
    }
}

impl ContextProvider for Functions {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        plan_err!("expressions can't read tables (`{name}`)")
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.planners
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        USER.get(name).or_else(|| INTERNAL.get(name)).cloned()
    }

    fn get_higher_order_meta(&self, _name: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        USER.keys().map(|k| k.to_string()).collect()
    }

    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// `int(x)`, `float(x)`, `string(x)`: strict conversions. A value that
/// doesn't convert exactly is an error, never a silent null or truncation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FerryCast {
    target: Target,
    signature: Signature,
}

impl FerryCast {
    fn new(target: Target) -> Self {
        Self {
            target,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FerryCast {
    fn name(&self) -> &str {
        self.target.function()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Err(reason) = convert::check(&arg_types[0], self.target) {
            return plan_err!("{}() {reason}", self.target.function());
        }
        Ok(self.target.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("{}() takes one argument", self.target.function());
        };
        let array = arg.to_array(args.number_rows)?;
        convert::convert(&array, self.target)
            .map(ColumnarValue::Array)
            .map_err(|e| {
                // The conversion's own message, without arrow's "Cast error: ".
                let message = match e {
                    arrow_schema::ArrowError::CastError(message) => message,
                    other => other.to_string(),
                };
                DataFusionError::Execution(format!("{}(): {message}", self.target.function()))
            })
    }
}

/// `items[*].sku`: the field of every struct in a list, as a list with the
/// same shape (same offsets, same null lists).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ListField {
    signature: Signature,
}

impl ListField {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

fn field_name(scalar: Option<&ScalarValue>) -> Result<&str> {
    match scalar {
        Some(ScalarValue::Utf8(Some(name))) => Ok(name),
        _ => plan_err!("{LIST_FIELD} needs a literal field name"),
    }
}

fn element_field<'a>(list: &'a DataType, name: &str) -> Result<(&'a FieldRef, usize, &'a Field)> {
    let DataType::List(item) = list else {
        return plan_err!("{LIST_FIELD} needs a list, got {list}");
    };
    let DataType::Struct(fields) = item.data_type() else {
        return plan_err!("{LIST_FIELD} needs a list of structs, got {list}");
    };
    match fields.find(name) {
        Some((index, field)) => Ok((item, index, field)),
        None => plan_err!("the list's elements have no field `{name}`"),
    }
}

impl ScalarUDFImpl for ListField {
    fn name(&self) -> &str {
        LIST_FIELD
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{LIST_FIELD}: use return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let name = field_name(args.scalar_arguments.get(1).copied().flatten())?;
        let list = &args.arg_fields[0];
        let (_, _, field) = element_field(list.data_type(), name)?;
        let item = Field::new_list_field(field.data_type().clone(), true)
            .with_metadata(field.metadata().clone());
        Ok(Arc::new(Field::new(
            LIST_FIELD,
            DataType::List(Arc::new(item)),
            list.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let name = match &args.args[1] {
            ColumnarValue::Scalar(scalar) => field_name(Some(scalar))?,
            ColumnarValue::Array(_) => return exec_err!("{LIST_FIELD} needs a literal field name"),
        };
        let array = args.args[0].to_array(args.number_rows)?;
        let (_, index, _) = element_field(array.data_type(), name)?;
        let lists = array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("checked to be a list");
        let structs = lists
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("checked to be a list of structs");
        // A null struct element gives a null value, not the struct's child.
        let values: ArrayRef = match structs.nulls() {
            Some(nulls) => {
                let child = structs.column(index).to_data();
                let merged = arrow_buffer::NullBuffer::union(Some(nulls), child.nulls());
                arrow_array::make_array(
                    child
                        .into_builder()
                        .nulls(merged)
                        .build()
                        .map_err(DataFusionError::from)?,
                )
            }
            None => structs.column(index).clone(),
        };
        let item = match args.return_field.data_type() {
            DataType::List(item) => item.clone(),
            other => return exec_err!("{LIST_FIELD} returns a list, not {other}"),
        };
        let out = ListArray::try_new(
            item,
            lists.offsets().clone(),
            values,
            lists.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_function_is_immutable() {
        for (name, udf) in USER.iter().chain(INTERNAL.iter()) {
            assert_eq!(
                udf.signature().volatility,
                Volatility::Immutable,
                "`{name}` must not depend on anything but its arguments"
            );
        }
    }

    #[test]
    fn clock_randomness_and_session_functions_are_unreachable() {
        let functions = Functions::new();
        for name in [
            "now",
            "random",
            "uuid",
            "current_date",
            "current_time",
            "to_timestamp",
            "version",
        ] {
            assert!(functions.get_function_meta(name).is_none(), "{name}");
        }
    }
}
