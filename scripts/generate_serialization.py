import os
import json
import re

source_base = os.path.sep.join('src/include/duckdb/storage/serialization'.split('/'))
target_base = os.path.sep.join('src/storage/serialization'.split('/'))

file_list = []
for fname in os.listdir(source_base):
    if '.json' not in fname:
        continue
    file_list.append(
        {
            'source': os.path.join(source_base, fname),
            'target': os.path.join(target_base, 'serialize_' + fname.replace('.json', '.cpp'))
        }
    )


include_base = '#include "${FILENAME}"\n'

header = '''//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//

${INCLUDE_LIST}
namespace duckdb {
'''

footer = '''
} // namespace duckdb
'''

serialize_base = '''
void ${CLASS_NAME}::FormatSerialize(FormatSerializer &serializer) const {
${MEMBERS}}
'''

serialize_element = '\tserializer.WriteProperty("${PROPERTY_KEY}", ${PROPERTY_NAME});\n'

base_serialize = '\t${BASE_CLASS_NAME}::FormatSerialize(serializer);\n'

pointer_return = '${POINTER}<${CLASS_NAME}>'

deserialize_base = '''
${DESERIALIZE_RETURN} ${CLASS_NAME}::FormatDeserialize(${EXTRA_PARAMETERS}FormatDeserializer &deserializer) {
${MEMBERS}
}
'''

switch_code = '''\tswitch (${SWITCH_VARIABLE}) {
${CASE_STATEMENTS}\tdefault:
\t\tthrow SerializationException("Unsupported type for deserialization of ${BASE_CLASS}!");
\t}
'''

switch_header = '\tcase ${ENUM_TYPE}::${ENUM_VALUE}:\n'

switch_statement = switch_header + '''\t\tresult = ${CLASS_DESERIALIZE}::FormatDeserialize(${EXTRA_PARAMETERS}deserializer);
\t\tbreak;
'''

deserialize_element = '\tauto ${PROPERTY_NAME} = deserializer.ReadProperty<${PROPERTY_TYPE}>("${PROPERTY_KEY}");\n'
deserialize_element_class = '\tdeserializer.ReadProperty("${PROPERTY_KEY}", result${ASSIGNMENT}${PROPERTY_NAME});\n'
deserialize_element_class_base = '\tauto ${PROPERTY_NAME} = deserializer.ReadProperty<unique_ptr<${BASE_PROPERTY}>>("${PROPERTY_KEY}");\n\tresult${ASSIGNMENT}${PROPERTY_NAME} = unique_ptr_cast<${BASE_PROPERTY}, ${DERIVED_PROPERTY}>(std::move(${PROPERTY_NAME}));\n'

move_list = [
    'string', 'ParsedExpression*', 'CommonTableExpressionMap'
]

def is_container(type):
    return '<' in type

def is_pointer(type):
    return type.endswith('*') or type.startswith('shared_ptr<')

def requires_move(type):
    return is_container(type) or is_pointer(type)

def replace_pointer(type):
    return re.sub('([a-zA-Z0-9]+)[*]', 'unique_ptr<\\1>', type)

def get_serialize_element(property_name, property_key, property_type, is_optional, pointer_type):
    write_method = 'WriteProperty'
    assignment = '.' if pointer_type == 'none' else '->'
    if is_optional:
        write_method = 'WriteOptionalProperty'
    return serialize_element.replace('${PROPERTY_NAME}', property_name).replace('${PROPERTY_KEY}', property_key).replace('WriteProperty', write_method).replace('${ASSIGNMENT}', assignment)

def get_deserialize_element_template(template, property_name, property_key, property_type, is_optional, pointer_type):
    read_method = 'ReadProperty'
    assignment = '.' if pointer_type == 'none' else '->'
    if is_optional:
        read_method = 'ReadOptionalProperty'
    return template.replace('${PROPERTY_NAME}', property_name).replace('${PROPERTY_KEY}', property_key).replace('ReadProperty', read_method).replace('${PROPERTY_TYPE}', property_type).replace('${ASSIGNMENT}', assignment)


def get_deserialize_element(property_name, property_key, property_type, is_optional, pointer_type):
    return get_deserialize_element_template(deserialize_element, property_name, property_key, property_type, is_optional, pointer_type)

def get_return_value(pointer_type, class_name):
    if pointer_type == 'none':
        return class_name
    return pointer_return.replace('${POINTER}', pointer_type).replace('${CLASS_NAME}', class_name)

def generate_constructor(pointer_type, class_name, constructor_parameters):
    if pointer_type == 'none':
        params = '' if len(constructor_parameters) == 0 else '(' + constructor_parameters + ')'
        return f'\t{class_name} result{params};\n'
    return f'\tauto result = duckdb::{pointer_type}<{class_name}>(new {class_name}({constructor_parameters}));\n'

class MemberVariable:
    def __init__(self, entry):
        self.name = entry['name']
        self.type = entry['type']
        self.base = None
        self.optional = False
        if 'property' in entry:
            self.serialize_property = entry['property']
            self.deserialize_property = entry['property']
        else:
            self.serialize_property = self.name
            self.deserialize_property = self.name
        if 'serialize_property' in entry:
            self.serialize_property = entry['serialize_property']
        if 'deserialize_property' in entry:
            self.deserialize_property = entry['deserialize_property']
        if 'optional' in entry:
            self.optional = entry['optional']
        if 'base' in entry:
            self.base = entry['base']

class SerializableClass:
    def __init__(self, entry):
        self.name = entry['class']
        self.is_base_class = 'class_type' in entry
        self.base = None
        self.base_object = None
        self.enum_value = None
        self.enum_entry = None
        self.extra_parameters = []
        self.pointer_type = 'unique_ptr'
        self.constructor = None
        self.members = None
        self.custom_implementation = False
        self.custom_switch_code = None
        self.children = {}
        self.return_type = self.name
        self.return_class = self.name
        if self.is_base_class:
            self.enum_value = entry['class_type']
        if 'extra_parameters' in entry:
            self.extra_parameters = entry['extra_parameters']
        if 'pointer_type' in entry:
            self.pointer_type = entry['pointer_type']
        if 'base' in entry:
            self.base = entry['base']
            self.enum_entry = entry['enum']
            self.return_type = self.base
        if 'constructor' in entry:
            self.constructor = entry['constructor']
        if 'custom_implementation' in entry and entry['custom_implementation']:
            self.custom_implementation = True
        if 'custom_switch_code' in entry:
            self.custom_switch_code = entry['custom_switch_code']
        if 'members' in entry:
            self.members = [MemberVariable(x) for x in entry['members']]
        if 'return_type' in entry:
            self.return_type = entry['return_type']
            self.return_class = self.return_type


    def inherit(self, base_class):
        self.base_object = base_class
        self.extra_parameters = base_class.extra_parameters
        self.pointer_type = base_class.pointer_type


def generate_base_class_code(base_class):
    base_class_serialize = ''
    base_class_deserialize = ''

    # properties
    enum_type = ''
    for entry in base_class.members:
        type_name = replace_pointer(entry.type)
        if entry.serialize_property == base_class.enum_value:
            enum_type = entry.type
        is_optional = entry.optional
        base_class_serialize += get_serialize_element(entry.serialize_property, entry.name, type_name, is_optional, base_class.pointer_type)
        base_class_deserialize += get_deserialize_element(entry.deserialize_property, entry.name, type_name, is_optional, base_class.pointer_type)
    expressions = [x for x in base_class.children.items()]
    expressions = sorted(expressions, key=lambda x: x[0])

    base_class_deserialize += f'\t{base_class.pointer_type}<{base_class.name}> result;\n'
    switch_cases = ''
    extra_parameter_txt = ''
    for extra_parameter in base_class.extra_parameters:
        extra_parameter_txt += extra_parameter + ', '
    for expr in expressions:
        enum_value = expr[0]
        child_data = expr[1]
        if child_data.custom_switch_code is not None:
            switch_cases += switch_header.replace('${ENUM_TYPE}', enum_type).replace('${ENUM_VALUE}', enum_value).replace('${CLASS_DESERIALIZE}', child_data.name)
            switch_cases += '\n'.join(['\t\t' + x for x in child_data.custom_switch_code.replace('\\n', '\n').split('\n')])
            switch_cases += '\n'
            continue
        switch_cases += switch_statement.replace('${ENUM_TYPE}', enum_type).replace('${ENUM_VALUE}', enum_value).replace('${CLASS_DESERIALIZE}', child_data.name).replace('${EXTRA_PARAMETERS}', extra_parameter_txt)

    assign_entries = []
    for entry in base_class.members:
        skip = False
        for check_entry in [entry.name, entry.serialize_property]:
            if check_entry in base_class.extra_parameters:
                skip = True
            if check_entry == base_class.enum_value:
                skip = True
        if skip:
            continue
        assign_entries.append(entry)

    # class switch statement
    base_class_deserialize += switch_code.replace('${SWITCH_VARIABLE}', base_class.enum_value).replace('${CASE_STATEMENTS}', switch_cases).replace('${BASE_CLASS}', base_class.name)

    deserialize_return = get_return_value(base_class.pointer_type, base_class.return_type)

    for entry in assign_entries:
        move = False
        if entry.type in move_list or is_container(entry.type) or is_pointer(entry.type):
            move = True
        if move:
            base_class_deserialize+= f'\tresult->{entry.deserialize_property} = std::move({entry.deserialize_property});\n'
        else:
            base_class_deserialize+= f'\tresult->{entry.deserialize_property} = {entry.deserialize_property};\n'
    base_class_deserialize += '\treturn result;'
    base_class_generation = ''
    base_class_generation += serialize_base.replace('${CLASS_NAME}', base_class.name).replace('${MEMBERS}', base_class_serialize)
    base_class_generation += deserialize_base.replace('${DESERIALIZE_RETURN}', deserialize_return).replace('${CLASS_NAME}', base_class.name).replace('${MEMBERS}', base_class_deserialize).replace('${EXTRA_PARAMETERS}', '')
    return base_class_generation


def generate_class_code(class_entry):
    if class_entry.custom_implementation:
        return None
    extra_parameters = []
    extra_parameter_txt = ''
    class_serialize = ''
    class_deserialize = ''
    if class_entry.base is not None:
        extra_parameters = class_entry.extra_parameters
        for extra_parameter in extra_parameters:
            extra_parameter_type = ''
            for member in class_entry.base_object.members:
                if member.name == extra_parameter:
                    extra_parameter_type = member.type
            if len(extra_parameter_type) == 0:
                raise Exception('Extra parameter type not found')
            extra_parameter_txt += extra_parameter_type + ' ' + extra_parameter + ', '

    constructor_parameters = ''
    constructor_entries = {}
    if class_entry.constructor is not None:
        for constructor_entry in class_entry.constructor:
            constructor_entries[constructor_entry] = True
            found = False
            for entry in class_entry.members:
                if entry.name == constructor_entry:
                    if len(constructor_parameters) > 0:
                        constructor_parameters += ", "
                    type_name = replace_pointer(entry.type)
                    if requires_move(type_name):
                        constructor_parameters += 'std::move(' + entry.name + ')'
                    else:
                        constructor_parameters += entry.name
                    class_deserialize += get_deserialize_element(entry.name, entry.name, type_name, entry.optional, 'unique_ptr')
                    found = True
                    break
            if not found:
                raise Exception(f"Constructor member \"{constructor_entry}\" was not found in members list")
    else:
        constructor_parameters = ', '.join(extra_parameters)

    if class_entry.base is not None:
        class_serialize += base_serialize.replace('${BASE_CLASS_NAME}', class_entry.base)
    class_deserialize += generate_constructor(class_entry.pointer_type, class_entry.return_class, constructor_parameters)
    if class_entry.members is None:
        return None
    for entry in class_entry.members:
        property_key = entry.name
        is_optional = False
        write_property_name = entry.serialize_property
        is_optional = entry.optional
        if is_pointer(entry.type):
            if not is_optional:
                write_property_name = '*' + entry.serialize_property
        elif is_optional:
            raise Exception(f"Optional can only be combined with pointers (in {class_entry.name}, type {entry.type}, member {entry.type})")
        deserialize_template_str = deserialize_element_class
        if entry.base:
            write_property_name = f"({entry.base} &)" + write_property_name
            deserialize_template_str = deserialize_element_class_base.replace('${BASE_PROPERTY}', entry.base.replace('*', '')).replace('${DERIVED_PROPERTY}', entry.type.replace('*', ''))
        type_name = replace_pointer(entry.type)
        class_serialize += get_serialize_element(write_property_name, property_key, type_name, is_optional, class_entry.pointer_type)
        if entry.name not in constructor_entries:
            class_deserialize += get_deserialize_element_template(deserialize_template_str, entry.deserialize_property, property_key, type_name, is_optional, class_entry.pointer_type)

    if class_entry.base is None:
        class_deserialize += '\treturn result;'
    else:
        class_deserialize += '\treturn std::move(result);'
    deserialize_return = get_return_value(class_entry.pointer_type, class_entry.return_type)

    class_generation = ''
    class_generation += serialize_base.replace('${CLASS_NAME}', class_entry.name).replace('${MEMBERS}', class_serialize)
    class_generation += deserialize_base.replace('${DESERIALIZE_RETURN}', deserialize_return).replace('${CLASS_NAME}', class_entry.name).replace('${MEMBERS}', class_deserialize).replace('${EXTRA_PARAMETERS}', extra_parameter_txt)
    return class_generation



for entry in file_list:
    source_path = entry['source']
    target_path = entry['target']
    with open(source_path, 'r') as f:
        json_data = json.load(f)

    include_list = ['duckdb/common/serializer/format_serializer.hpp', 'duckdb/common/serializer/format_deserializer.hpp']
    base_classes = []
    classes = []
    base_class_data = {}

    for entry in json_data:
        if 'includes' in entry:
            include_list += entry['includes']
        new_class = SerializableClass(entry)
        if new_class.is_base_class:
            # this class is a base class itself - construct the base class list
            if new_class.name in base_class_data:
                raise Exception(f"Duplicate base class \"{new_class.name}\"")
            base_class_data[new_class.name] = new_class
            base_classes.append(new_class)
        else:
            classes.append(new_class)
            if new_class.base is not None:
                # this class inherits from a base class - add the enum value
                if new_class.base not in base_class_data:
                    raise Exception(f"Unknown base class \"{new_class.base}\" for entry \"{new_class.name}\"")
                base_class_object = base_class_data[new_class.base]
                if new_class.enum_entry in base_class_object.children:
                    raise Exception(f"Duplicate enum entry \"{new_class.enum_entry}\"")
                new_class.inherit(base_class_object)
                base_class_object.children[new_class.enum_entry] = new_class

    with open(target_path, 'w+') as f:
        f.write(header.replace('${INCLUDE_LIST}', ''.join([include_base.replace('${FILENAME}', x) for x in include_list])))

        # generate the base class serialization
        for base_class in base_classes:
            base_class_generation = generate_base_class_code(base_class)
            f.write(base_class_generation)

        # generate the class serialization
        classes = sorted(classes, key = lambda x: x.name)
        for class_entry in classes:
            class_generation = generate_class_code(class_entry)
            if class_generation is None:
                continue
            f.write(class_generation)

        f.write(footer)
