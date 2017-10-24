// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

#include <graphqlparser/Ast.h>
#include <graphqlparser/AstVisitor.h>
#include <graphqlparser/GraphQLParser.h>

#include <mesos/v1/resources.hpp>

#include <mesos/v1/master/master.hpp>

#include <stout/error.hpp>
#include <stout/jsonify.hpp>
#include <stout/option.hpp>
#include <stout/try.hpp>

namespace graphql {

/**
 * @return JSON string that results from executing the GraphQL query.
 */
Option<Error> execute(
    const std::string& s,
    const google::protobuf::Message& message,
    JSON::ObjectWriter* writer);


namespace internal {

/**
 * Attempts to match the GraphQL `value` with the field named `name`
 * within the specified protobuf `message`.
 *
 * A "match" is considered successful if the `value` is equal to or a
 * subset of protobuf field in the case of repeated fields and
 * messages.
 *
 * @return true if the value matches otherwise false.
 */
Try<bool> match(
    const google::protobuf::Message& message,
    const std::string& name,
    const facebook::graphql::ast::Value& value);


class ValueVisitor : public facebook::graphql::ast::visitor::AstVisitor
{
public:
  ValueVisitor(bool* matches, Option<Error>* error, const std::string& type)
    : matches(matches), error(error), type(type) {}

  bool visitVariable(const facebook::graphql::ast::Variable& variable) override
  {
    *error = Error(
        "Variables (e.g., $" + std::string(variable.getName().getValue()) +
        ") are not currently supported");
    return false;
  }

  bool visitIntValue(const facebook::graphql::ast::IntValue& intValue) override
  {
    *error = Error(
        "Cannot use int value (e.g., " +
        std::string(intValue.getValue()) +
        ") where " + type + " expected");
    return false;
  }

  bool visitFloatValue(
      const facebook::graphql::ast::FloatValue& floatValue) override
  {
    *error = Error(
        "Cannot use float value (e.g., " +
        std::string(floatValue.getValue()) +
        ") where " + type + " expected");
    return false;
  }

  bool visitStringValue(
      const facebook::graphql::ast::StringValue& stringValue) override
  {
    *error = Error(
        "Cannot use string value (e.g., " +
        std::string(stringValue.getValue()) +
        ") where " + type + " expected");
    return false;
  }

  bool visitBooleanValue(
      const facebook::graphql::ast::BooleanValue& booleanValue) override
  {
    *error = Error(
        "Cannot use boolean value where " + type + " expected");
    return false;
  }

  bool visitNullValue(
      const facebook::graphql::ast::NullValue& nullValue) override
  {
    *error = Error("Null values are not currently supported");
    return false;
  }

  bool visitEnumValue(
      const facebook::graphql::ast::EnumValue& enumValue) override
  {
    *error = Error(
        "Cannot use enum value (e.g., " +
        std::string(enumValue.getValue()) +
        ") where " + type + " expected");
    return false;
  }

  bool visitListValue(
      const facebook::graphql::ast::ListValue& listValue) override
  {
    *error = Error("Cannot use list value where " + type + " expected");
    return false;
  }

  bool visitObjectValue(
      const facebook::graphql::ast::ObjectValue& objectValue) override
  {
    *error = Error("Cannot use object value where " + type + " expected");
    return false;
  }

protected:
  bool* matches;
  Option<Error>* error;
  const std::string type;
};


class BooleanValueVisitor : public ValueVisitor
{
public:
  BooleanValueVisitor(bool b, bool* matches, Option<Error>* error)
    : ValueVisitor(matches, error, "boolean"), b(b) {}

  bool visitBooleanValue(
      const facebook::graphql::ast::BooleanValue& booleanValue) override
  {
    if (booleanValue.getValue() == b) {
      *matches = true;
    }
    return false;
  }

private:
  bool b;
};


template <typename V>
class IntValueVisitor : public ValueVisitor
{
public:
  IntValueVisitor(V v, bool* matches, Option<Error>* error)
    : ValueVisitor(matches, error, type()), v(v) {}

  static std::string type()
  {
    if (std::is_same<V, int32_t>()) {
      return "32-bit int";
    } else if (std::is_same<V, uint32_t>()) {
      return "unsigned 32-bit int";
    } else if (std::is_same<V, uint64_t>()) {
      return "64-bit int";
    } else if (std::is_same<V, uint32_t>()) {
      return "unsigned 64-bit int";
    }
    return "int";
  }

  bool visitIntValue(const facebook::graphql::ast::IntValue& intValue) override
  {
    Try<V> value = numify<V>(intValue.getValue());
    if (value.isError()) {
      *error = Error(value.error());
    } else if (value.get() == v) {
      *matches = true;
    }
    return false;
  }

private:
  V v;
};


template <typename V>
class FloatValueVisitor : public ValueVisitor
{
public:
  FloatValueVisitor(V v, bool* matches, Option<Error>* error)
    : ValueVisitor(matches, error, type()), v(v) {}

  static std::string type()
  {
    if (std::is_same<V, double>()) {
      return "double";
    }
    return "float";
  }

  bool visitFloatValue(
      const facebook::graphql::ast::FloatValue& floatValue) override
  {
    Try<V> value = numify<V>(floatValue.getValue());
    if (value.isError()) {
      *error = Error(value.error());
    } else if (value.get() == v) { // TODO(benh): Make this approximate equal.
      *matches = true;
    }
    return false;
  }

private:
  V v;
};


class ObjectValueVisitor : public ValueVisitor
{
public:
  ObjectValueVisitor(
      const google::protobuf::Message& message,
      bool* matches,
      Option<Error>* error)
    : ValueVisitor(matches, error, "message"), message(message) {}

  bool visitObjectValue(
      const facebook::graphql::ast::ObjectValue& objectValue) override
  {
    foreach (auto&& field, objectValue.getFields()) {
      Try<bool> matches = match(
          message,
          field->getName().getValue(),
          field->getValue());

      if (matches.isError()) {
        *error = Error(matches.error());
        return false;
      } else if (!matches.get()) {
        return false;
      }
    }

    // We haven't returned which means all the fields must have matched!
    *matches = true;

    return false;
  }

private:
  const google::protobuf::Message& message;
};


class EnumValueVisitor : public ValueVisitor
{
public:
  EnumValueVisitor(const std::string& name, bool* matches, Option<Error>* error)
    : ValueVisitor(matches, error, "enum"), name(name) {}

  bool visitEnumValue(
      const facebook::graphql::ast::EnumValue& enumValue) override
  {
    if (enumValue.getValue() == name) {
      *matches = true;
    }
    return false;
  }

private:
  const std::string& name;
};


class StringValueVisitor : public ValueVisitor
{
public:
  StringValueVisitor(const std::string& s, bool* matches, Option<Error>* error)
    : ValueVisitor(matches, error, "string"), s(s) {}

  bool visitStringValue(
      const facebook::graphql::ast::StringValue& stringValue) override
  {
    if (stringValue.getValue() == s) {
      *matches = true;
    }
    return false;
  }

private:
  const std::string& s;
};


class ListValueVisitor : public ValueVisitor
{
public:
  ListValueVisitor(
      const google::protobuf::Message& message,
      const google::protobuf::FieldDescriptor* field,
      bool* matches,
      Option<Error>* error)
    : ValueVisitor(matches, error, "repeated"),
      message(message),
      field(field),
      reflection(CHECK_NOTNULL(message.GetReflection()))
  {
    CHECK(field->is_repeated());
  }

  bool visitListValue(
      const facebook::graphql::ast::ListValue& listValue) override
  {
    // For every value a match must be _found_ in at least one element.
    //
    // TODO(benh): If N different instances of `value` are exactly
    // the same should we try and find N elements that match? For
    // example, a list value of [1, 2, 3, 3] currently matches the
    // list [1, 2, 3, 4] as well as [1, 2, 3, 3, 4] but we could
    // adopt semantics where only [1, 2, 3, 3, 4] would match.
    bool found = true;

    foreach (auto&& value, listValue.getValues()) {
      bool matches = false;
      int fieldSize = reflection->FieldSize(message, field);
      for (int i = 0; i < fieldSize; ++i) {
        if (matches) {
          break;
        }
        switch (field->cpp_type()) {
          using google::protobuf::FieldDescriptor;
          case FieldDescriptor::CPPTYPE_BOOL: {
            BooleanValueVisitor visitor(
                reflection->GetRepeatedBool(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_INT32: {
            IntValueVisitor<int32_t> visitor(
                reflection->GetRepeatedInt32(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_INT64: {
            IntValueVisitor<int64_t> visitor(
                reflection->GetRepeatedInt64(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_UINT32: {
            IntValueVisitor<uint32_t> visitor(
                reflection->GetRepeatedUInt32(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_UINT64: {
            IntValueVisitor<uint64_t> visitor(
                reflection->GetRepeatedUInt64(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_FLOAT: {
            FloatValueVisitor<float> visitor(
                reflection->GetRepeatedFloat(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_DOUBLE: {
            FloatValueVisitor<double> visitor(
                reflection->GetRepeatedDouble(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_MESSAGE: {
            ObjectValueVisitor visitor(
                reflection->GetRepeatedMessage(message, field, i),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_ENUM: {
            EnumValueVisitor visitor(
                reflection->GetRepeatedEnum(message, field, i)->name(),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
          case FieldDescriptor::CPPTYPE_STRING: {
            StringValueVisitor visitor(
                reflection->GetRepeatedStringReference(
                    message, field, i, nullptr),
                &matches,
                error);
            value->accept(&visitor);
            break;
          }
        }

        if (error->isSome()) {
          return false;
        } else if (matches) {
          break; // No longer need to keep looking for a match!
        }
      }

      if (!matches) {
        found = false;
        break;
      }
    }

    *matches = found;

    return false;
  }

private:
  const google::protobuf::Message& message;
  const google::protobuf::FieldDescriptor* field;
  const google::protobuf::Reflection* reflection;
};


inline Try<bool> match(
    const google::protobuf::Message& message,
    const std::string& name,
    const facebook::graphql::ast::Value& value)
{
  const google::protobuf::FieldDescriptor* field =
    message.GetDescriptor()->FindFieldByName(name);

  if (field == nullptr) {
    return Error("Unknown field: " + name); // TODO(benh): Add message name.
  }

  bool matches = false; // Assume we don't match to start!

  Option<Error> error = None();

  if (field->is_repeated()) {
    ListValueVisitor visitor(message, field, &matches, &error);
    value.accept(&visitor);
  } else {
    const google::protobuf::Reflection* reflection = message.GetReflection();
    using google::protobuf::FieldDescriptor;
    switch (field->cpp_type()) {
      case FieldDescriptor::CPPTYPE_BOOL: {
        BooleanValueVisitor visitor(
            reflection->GetBool(message, field),
            &matches,
            &error);
         value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_INT32: {
        IntValueVisitor<int32_t> visitor(
            reflection->GetInt32(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_INT64: {
        IntValueVisitor<int64_t> visitor(
            reflection->GetInt64(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_UINT32: {
        IntValueVisitor<uint32_t> visitor(
            reflection->GetUInt32(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_UINT64: {
        IntValueVisitor<uint64_t> visitor(
            reflection->GetUInt64(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_FLOAT: {
        FloatValueVisitor<float> visitor(
            reflection->GetFloat(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_DOUBLE: {
        FloatValueVisitor<double> visitor(
            reflection->GetDouble(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_MESSAGE: {
        ObjectValueVisitor visitor(
            reflection->GetMessage(message, field),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_ENUM: {
        EnumValueVisitor visitor(
            reflection->GetEnum(message, field)->name(),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
      case FieldDescriptor::CPPTYPE_STRING: {
        StringValueVisitor visitor(
            reflection->GetStringReference(message, field, nullptr),
            &matches,
            &error);
        value.accept(&visitor);
        break;
      }
    }
  }

  if (error.isSome()) {
    return error.get();
  }

  return matches;
}


class Visitor : public facebook::graphql::ast::visitor::AstVisitor
{
public:
  Visitor(
      const google::protobuf::Message& message,
      JSON::ObjectWriter* writer,
      Option<Error>* error)
    : message(message),
      writer(writer),
      error(error) {}

  bool visitOperationDefinition(
      const facebook::graphql::ast::OperationDefinition& operationDefinition)
    override
  {
    if (error->isSome()) {
      return false;
    }

    const std::string QUERY = "query";

    if (operationDefinition.getOperation() != QUERY) {
      *error = Error("Only the 'query' operation is currently supported");
      return false;
    }

    // Ensure there are no variable definitions since we don't support
    // them right now.
    auto variableDefinitions = operationDefinition.getVariableDefinitions();

    if (variableDefinitions != nullptr) {
      if (!variableDefinitions->empty()) {
        *error = Error("Variable definitions are not currently supported");
        return false;
      }
    }

    return true;
  }

  bool visitField(const facebook::graphql::ast::Field& field) override
  {
    if (error->isSome()) {
      return false;
    }

    // TODO(benh): Handle 'alias' and 'directives'.

    return resolve(
        field.getName().getValue(),
        field.getArguments(),
        field.getSelectionSet());
  }

private:
  bool resolve(
      const std::string& name,
      const std::vector<
          std::unique_ptr<facebook::graphql::ast::Argument>>* arguments,
      const facebook::graphql::ast::SelectionSet* selectionSet)
  {
    using google::protobuf::FieldDescriptor;

    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();

    const FieldDescriptor* field = descriptor->FindFieldByName(name);

    if (field == nullptr) {
      *error = Error("Unknown field: " + name); // TODO(benh): Add message name.
      return false;
    }

    if (arguments != nullptr &&
        (!field->is_repeated() ||
         field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE)) {
      *error = Error("Arguments are only supported on repeated messages");
      return false;
    }

    if (selectionSet != nullptr &&
        field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
      *error = Error("Can not use selection set on scalars");
      return false;
    }

    const google::protobuf::Reflection* reflection = message.GetReflection();

    if (field->is_repeated()) {
      writer->field(
          name,
          [this, &name, &field, &reflection, &arguments, &selectionSet](
              JSON::ArrayWriter* writer) {
            int fieldSize = reflection->FieldSize(message, field);
            for (int i = 0; i < fieldSize; ++i) {
              switch (field->cpp_type()) {
                case FieldDescriptor::CPPTYPE_BOOL:
                  writer->element(
                      reflection->GetRepeatedBool(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_INT32:
                  writer->element(
                      reflection->GetRepeatedInt32(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_INT64:
                  writer->element(
                      reflection->GetRepeatedInt64(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_UINT32:
                  writer->element(
                      reflection->GetRepeatedUInt32(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_UINT64:
                  writer->element(
                      reflection->GetRepeatedUInt64(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_FLOAT:
                  writer->element(
                      reflection->GetRepeatedFloat(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_DOUBLE:
                  writer->element(
                      reflection->GetRepeatedDouble(message, field, i));
                  break;
                case FieldDescriptor::CPPTYPE_MESSAGE: {
                  auto&& element =
                    reflection->GetRepeatedMessage(message, field, i);

                  // Filter out this message if the arguments don't match.
                  if (arguments != nullptr) {
                    bool matches = false;
                    foreach (auto&& argument, *arguments) {
                      if (argument->getName().getValue() ==
                          std::string("matches")) {
                        // TODO(benh): What if we want to match
                        // something that's not an object?
                        ObjectValueVisitor visitor(
                            element,
                            &matches,
                            error);
                        argument->getValue().accept(&visitor);
                        if (error->isSome()) {
                          return;
                        } else if (!matches) {
                          break;
                        }
                      }
                    }
                    if (!matches) {
                      continue;
                    }
                  }

                  if (selectionSet != nullptr) {
                    writer->element(
                        [this, &selectionSet, &element](
                            JSON::ObjectWriter* writer) {
                          Visitor visitor(element, writer, error);
                          selectionSet->accept(&visitor);
                        });
                  } else {
                    writer->element(JSON::Protobuf(element));
                  }
                  break;
                }
                case FieldDescriptor::CPPTYPE_ENUM:
                  writer->element(
                      reflection->GetRepeatedEnum(message, field, i)->name());
                  break;
                case FieldDescriptor::CPPTYPE_STRING:
                  const std::string& s = reflection->GetRepeatedStringReference(
                      message, field, i, nullptr);
                  if (field->type() == FieldDescriptor::TYPE_BYTES) {
                    writer->element(base64::encode(s));
                  } else {
                    writer->element(s);
                  }
                  break;
              }
            }
          });
    } else if (
        reflection->HasField(message, field) ||
        (field->has_default_value() && !field->options().deprecated())) {
      switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_BOOL:
          writer->field(name, reflection->GetBool(message, field));
          break;
        case FieldDescriptor::CPPTYPE_INT32:
          writer->field(name, reflection->GetInt32(message, field));
          break;
        case FieldDescriptor::CPPTYPE_INT64:
          writer->field(name, reflection->GetInt64(message, field));
          break;
        case FieldDescriptor::CPPTYPE_UINT32:
          writer->field(name, reflection->GetUInt32(message, field));
          break;
        case FieldDescriptor::CPPTYPE_UINT64:
          writer->field(name, reflection->GetUInt64(message, field));
          break;
        case FieldDescriptor::CPPTYPE_FLOAT:
          writer->field(name, reflection->GetFloat(message, field));
          break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
          writer->field(name, reflection->GetDouble(message, field));
          break;
        case FieldDescriptor::CPPTYPE_MESSAGE: {
          auto&& nestedMessage = reflection->GetMessage(message, field);
          if (selectionSet != nullptr) {
            writer->field(
                name,
                [this, &selectionSet, &nestedMessage](
                    JSON::ObjectWriter* writer) {
                  Visitor visitor(nestedMessage, writer, error);
                  selectionSet->accept(&visitor);
                });
          } else {
            writer->field(name, JSON::Protobuf(nestedMessage));
          }
          break;
        }
        case FieldDescriptor::CPPTYPE_ENUM:
          writer->field(
              name, reflection->GetEnum(message, field)->name());
          break;
        case FieldDescriptor::CPPTYPE_STRING:
          const std::string& s = reflection->GetStringReference(
              message, field, nullptr);
          if (field->type() == FieldDescriptor::TYPE_BYTES) {
            writer->field(name, base64::encode(s));
          } else {
            writer->field(name, s);
          }
          break;
      }
    }

    // NOTE: must return false here because we already visited the
    // rest of the structure manually (nested visitors). Moreover,
    // continuing to visit would fail because `message` would no
    // longer make sense (i.e., we'd likely come across an unknown
    // field).
    return false;
  }

  const google::protobuf::Message& message;
  JSON::ObjectWriter* writer;
  Option<Error>* error;
};


inline Try<std::unique_ptr<facebook::graphql::ast::Node>> parse(
    const std::string& s)
{
  const char* document = s.c_str();
  const char* error = nullptr;

  std::unique_ptr<facebook::graphql::ast::Node> ast =
    facebook::graphql::parseString(document, &error);

  if (error != nullptr) {
    Error e = Error(error);
    std::free((void*) error);
    return e;
  }

  return std::move(ast);
}

} // namespace internal {


inline Option<Error> execute(
    const std::string& s,
    const google::protobuf::Message& message,
    JSON::ObjectWriter* writer)
{
  Try<std::unique_ptr<facebook::graphql::ast::Node>> ast = internal::parse(s);

  if (ast.isError()) {
    return Error(ast.error());
  }

  Option<Error> error = None();

  internal::Visitor visitor(message, writer, &error);

  ast.get()->accept(&visitor);

  return error;
}

} // namespace graphql {
