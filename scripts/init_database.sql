CREATE TABLE "location_types" (
  "id" serial,
  "name" text UNIQUE,
  PRIMARY KEY ("id")
);

CREATE TABLE "locations" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "parent_uuid" uuid,
  "name_ru" text,
  "name_kk" text,
  "type_id" int,
  "path" text,
  "external_id" text,
  "external_parent_id" text,
  "kato_code" text,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_locations.type_id"
    FOREIGN KEY ("type_id")
      REFERENCES "location_types"("id")
);

CREATE TABLE "mime_types" (
  "id" serial,
  "name" text UNIQUE,
  PRIMARY KEY ("id")
);

CREATE TABLE "files" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "name" text,
  "hash" text,
  "mime_type_id" int,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_files.mime_type_id"
    FOREIGN KEY ("mime_type_id")
      REFERENCES "mime_types"("id")
);

CREATE TABLE "organization_types" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "name_ru" text,
  "name_kk" text,
  "is_unit" boolean,
  "accept_appeal" boolean,
  PRIMARY KEY ("id")
);

CREATE TABLE "petition_languages" (
  "id" serial,
  "name" text UNIQUE,
  PRIMARY KEY ("id")
);

CREATE TABLE "petition_sources" (
  "id" serial,
  "name" text UNIQUE,
  PRIMARY KEY ("id")
);

CREATE TABLE "petition_states" (
  "id" serial,
  "name" text UNIQUE,
  PRIMARY KEY ("id")
);

CREATE TABLE "organizations" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "name_ru" text,
  "name_kk" text,
  "parent_uuid" uuid,
  "type_id" int,
  "location_id" int,
  "path" text,
  "has_unit" boolean,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_organizations.type_id"
    FOREIGN KEY ("type_id")
      REFERENCES "organization_types"("id"),
  CONSTRAINT "FK_organizations.location_id"
    FOREIGN KEY ("location_id")
      REFERENCES "locations"("id")
);

CREATE TABLE "petitions" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "title" text,
  "description" text,
  "reg_number" text,
  "state_id" int,
  "source_id" int,
  "apply_date" timestamp,
  "deadline" timestamp,
  "signers_count" int,
  "required_count" int,
  "viewers_count" int,
  "cover_file_id" int,
  "applicant_first_name" text,
  "applicant_last_name" text,
  "organization_id" int,
  "language_id" int,
  "location_latitude" text,
  "location_longitude" text,
  "location_address" text,
  "decision_message_kk" text,
  "decision_message_ru" text,
  "decision_reply_date" timestamp,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_petitions.cover_file_id"
    FOREIGN KEY ("cover_file_id")
      REFERENCES "files"("id"),
  CONSTRAINT "FK_petitions.language_id"
    FOREIGN KEY ("language_id")
      REFERENCES "petition_languages"("id"),
  CONSTRAINT "FK_petitions.source_id"
    FOREIGN KEY ("source_id")
      REFERENCES "petition_sources"("id"),
  CONSTRAINT "FK_petitions.state_id"
    FOREIGN KEY ("state_id")
      REFERENCES "petition_states"("id"),
  CONSTRAINT "FK_petitions.organization_id"
    FOREIGN KEY ("organization_id")
      REFERENCES "organizations"("id")
);

CREATE TABLE "petition_files" (
  "id" serial,
  "petition_id" int,
  "file_id" int,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_petition_files.file_id"
    FOREIGN KEY ("file_id")
      REFERENCES "files"("id"),
  CONSTRAINT "FK_petition_files.petition_id"
    FOREIGN KEY ("petition_id")
      REFERENCES "petitions"("id")
);

CREATE TABLE "signers" (
  "id" serial,
  "petition_uuid" uuid,
  "fio" text,
  "created_date" timestamp,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_signers.petition_uuid"
    FOREIGN KEY ("petition_uuid")
      REFERENCES "petitions"("uuid")
);

CREATE TABLE "comments" (
  "id" serial,
  "uuid" uuid UNIQUE,
  "petition_uuid" uuid,
  "parent_uuid" uuid,
  "fio" text,
  "comment" text,
  "replies_count" int,
  "created_date" timestamp,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_comments.petition_uuid"
    FOREIGN KEY ("petition_uuid")
      REFERENCES "petitions"("uuid"),
  CONSTRAINT "FK_comments.parent_uuid"
    FOREIGN KEY ("parent_uuid")
      REFERENCES "comments"("uuid")
);

CREATE TABLE "decision_files" (
  "id" serial,
  "petition_id" int,
  "file_id" int,
  PRIMARY KEY ("id"),
  CONSTRAINT "FK_decision_files.petition_id"
    FOREIGN KEY ("petition_id")
      REFERENCES "petitions"("id"),
  CONSTRAINT "FK_decision_files.file_id"
    FOREIGN KEY ("file_id")
      REFERENCES "files"("id")
);

