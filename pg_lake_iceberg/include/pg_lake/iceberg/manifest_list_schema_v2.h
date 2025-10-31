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

/*
 * Iceberg v2 version of the manifest list schema. Slightly modified version of below schema:
 *
 * https://github.com/apache/iceberg-rust/blob/f30d8723f4fc6038272cf8ad6beca65ce83d1ea6/crates/iceberg/testdata/avro_schema_manifest_file_v2.json
 *
 * Note: It was missing a few fields or the field names were different.
 */

static const char *manifest_list_schema_v2_json = "\
  { \
    \"type\": \"record\", \
    \"name\": \"manifest_file\", \
    \"fields\": [ \
      { \
        \"name\": \"manifest_path\", \
        \"type\": \"string\", \
        \"doc\": \"Location URI with FS scheme\", \
        \"field-id\": 500 \
      }, \
      { \
        \"name\": \"manifest_length\", \
        \"type\": \"long\", \
        \"doc\": \"Total file size in bytes\", \
        \"field-id\": 501 \
      }, \
      { \
        \"name\": \"partition_spec_id\", \
        \"type\": \"int\", \
        \"doc\": \"Spec ID used to write\", \
        \"field-id\": 502 \
      }, \
      { \
        \"name\": \"content\", \
        \"type\": \"int\", \
        \"doc\": \"Contents of the manifest: 0=data, 1=deletes\", \
        \"field-id\": 517 \
      }, \
      { \
        \"name\": \"sequence_number\", \
        \"type\": \"long\", \
        \"doc\": \"Sequence number when the manifest was added\", \
        \"field-id\": 515 \
      }, \
      { \
        \"name\": \"min_sequence_number\", \
        \"type\": \"long\", \
        \"doc\": \"Lowest sequence number in the manifest\", \
        \"field-id\": 516 \
      }, \
      { \
        \"name\": \"added_snapshot_id\", \
        \"type\": \"long\", \
        \"doc\": \"Snapshot ID that added the manifest\", \
        \"field-id\": 503 \
      }, \
      { \
        \"name\": \"added_files_count\", \
        \"type\": \"int\", \
        \"doc\": \"Added entry count\", \
        \"field-id\": 504 \
      }, \
      { \
        \"name\": \"existing_files_count\", \
        \"type\": \"int\", \
        \"doc\": \"Existing entry count\", \
        \"field-id\": 505 \
      }, \
      { \
        \"name\": \"deleted_files_count\", \
        \"type\": \"int\", \
        \"doc\": \"Deleted entry count\", \
        \"field-id\": 506 \
      }, \
      { \
        \"name\": \"added_rows_count\", \
        \"type\": \"long\", \
        \"doc\": \"Added rows count\", \
        \"field-id\": 512 \
      }, \
      { \
        \"name\": \"existing_rows_count\", \
        \"type\": \"long\", \
        \"doc\": \"Existing rows count\", \
        \"field-id\": 513 \
      }, \
      { \
        \"name\": \"deleted_rows_count\", \
        \"type\": \"long\", \
        \"doc\": \"Deleted rows count\", \
        \"field-id\": 514 \
      }, \
      { \
        \"name\": \"partitions\", \
        \"type\": [ \
          \"null\", \
          { \
            \"type\": \"array\", \
            \"items\": { \
              \"type\": \"record\", \
              \"name\": \"r508\", \
              \"fields\": [ \
                { \
                  \"name\": \"contains_null\", \
                  \"type\": \"boolean\", \
                  \"doc\": \"True if any file has a null partition value\", \
                  \"field-id\": 509 \
                }, \
                { \
                  \"name\": \"contains_nan\", \
                  \"type\": [ \
                    \"null\", \
                    \"boolean\" \
                  ], \
                  \"doc\": \"True if any file has a nan partition value\", \
                  \"field-id\": 518 \
                }, \
                { \
                  \"name\": \"lower_bound\", \
                  \"type\": [ \
                    \"null\", \
                    \"bytes\" \
                  ], \
                  \"doc\": \"Partition lower bound for all files\", \
                  \"field-id\": 510 \
                }, \
                { \
                  \"name\": \"upper_bound\", \
                  \"type\": [ \
                    \"null\", \
                    \"bytes\" \
                  ], \
                  \"doc\": \"Partition upper bound for all files\", \
                  \"field-id\": 511 \
                } \
              ] \
            }, \
            \"element-id\": 508 \
          } \
        ], \
        \"doc\": \"Summary for each partition\", \
        \"field-id\": 507 \
      }, \
      { \
        \"name\" : \"key_metadata\", \
        \"type\" : [\"null\",\"bytes\"], \
        \"default\" : null, \
        \"field-id\" : 519 \
      } \
    ] \
  }";
