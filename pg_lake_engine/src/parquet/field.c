/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "postgres.h"

#include "common/int.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/parquet/leaf_field.h"

static FieldStructElement * DeepCopyFieldStructElement(FieldStructElement * structElementField);
static Field * DeepCopyField(const Field * field);

/*
 * DeepCopyField deep copies a Field.
 */
static Field *
DeepCopyField(const Field * field)
{
	Field	   *fieldCopy = palloc0(sizeof(Field));

	fieldCopy->type = field->type;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				fieldCopy->field.scalar.typeName = pstrdup(field->field.scalar.typeName);
				break;
			}
		case FIELD_TYPE_LIST:
			{
				fieldCopy->field.list.element = DeepCopyField(field->field.list.element);
				fieldCopy->field.list.elementId = field->field.list.elementId;
				fieldCopy->field.list.elementRequired = field->field.list.elementRequired;
				break;
			}
		case FIELD_TYPE_MAP:
			{
				fieldCopy->field.map.key = DeepCopyField(field->field.map.key);
				fieldCopy->field.map.keyId = field->field.map.keyId;

				fieldCopy->field.map.value = DeepCopyField(field->field.map.value);
				fieldCopy->field.map.valueId = field->field.map.valueId;
				fieldCopy->field.map.valueRequired = field->field.map.valueRequired;
				break;
			}
		case FIELD_TYPE_STRUCT:
			{
				fieldCopy->field.structType.fields = palloc0(field->field.structType.nfields * sizeof(FieldStructElement));
				fieldCopy->field.structType.nfields = field->field.structType.nfields;

				for (size_t i = 0; i < field->field.structType.nfields; i++)
				{
					FieldStructElement *structElementField = &field->field.structType.fields[i];
					FieldStructElement *structElementFieldCopy = DeepCopyFieldStructElement(structElementField);

					fieldCopy->field.structType.fields[i] = *structElementFieldCopy;
				}

				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("invalid field type")));
			}
	}

	return fieldCopy;
}


/*
 * DeepCopyFieldStructElement deep copies a FieldStructElement.
 */
static FieldStructElement *
DeepCopyFieldStructElement(FieldStructElement * structElementField)
{
	FieldStructElement *copiedStructElementField = palloc0(sizeof(FieldStructElement));

	copiedStructElementField->id = structElementField->id;
	copiedStructElementField->name = pstrdup(structElementField->name);
	copiedStructElementField->required = structElementField->required;
	copiedStructElementField->doc = (structElementField->doc) ? pstrdup(structElementField->doc) : NULL;
	copiedStructElementField->writeDefault = (structElementField->writeDefault) ? pstrdup(structElementField->writeDefault) : NULL;
	copiedStructElementField->initialDefault = (structElementField->initialDefault) ? pstrdup(structElementField->initialDefault) : NULL;
	copiedStructElementField->duckSerializedInitialDefault = (structElementField->duckSerializedInitialDefault) ? pstrdup(structElementField->duckSerializedInitialDefault) : NULL;
	copiedStructElementField->type = DeepCopyField(structElementField->type);

	return copiedStructElementField;
}


/*
 * DeepCopyDataFileSchema deep copies a DataFileSchema.
 */
DataFileSchema *
DeepCopyDataFileSchema(const DataFileSchema * schema)
{
	DataFileSchema *copiedSchema = palloc0(sizeof(DataFileSchema));

	copiedSchema->fields = palloc0(schema->nfields * sizeof(DataFileSchemaField));
	copiedSchema->nfields = schema->nfields;

	for (size_t i = 0; i < schema->nfields; i++)
	{
		DataFileSchemaField *field = &schema->fields[i];
		DataFileSchemaField *fieldCopy = DeepCopyFieldStructElement(field);

		copiedSchema->fields[i] = *fieldCopy;
	}

	return copiedSchema;
}

int
LeafFieldCompare(const ListCell *a, const ListCell *b)
{
	LeafField  *fieldA = lfirst(a);
	LeafField  *fieldB = lfirst(b);

	return pg_cmp_s32(fieldA->fieldId, fieldB->fieldId);
}

#if PG_VERSION_NUM < 170000

int
pg_cmp_s32(int32 a, int32 b)
{
	return (a > b) - (a < b);
}
#endif
