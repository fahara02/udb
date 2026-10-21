erDiagram

    PATIENT ||--o{ PATIENT_ADDRESS : has
    PATIENT ||--o{ PATIENT_CONTACT : has
    PATIENT ||--o{ EMERGENCY_CONTACT : has
    PATIENT ||--o{ PATIENT_IDENTIFIER : has

    PATIENT ||--o{ PATIENT_MERGE : "source patient"
    PATIENT ||--o{ PATIENT_MERGE : "target patient"


    PATIENT {
        uuid id PK
        varchar mrn UK
        varchar first_name
        varchar middle_name
        varchar last_name
        date date_of_birth
        varchar sex
        varchar marital_status
        varchar registration_status
        timestamp deceased_at
        timestamp created_at
        timestamp updated_at
    }

    PATIENT_ADDRESS {
        uuid id PK
        uuid patient_id FK
        varchar address_type
        varchar address_line_1
        varchar address_line_2
        varchar city
        varchar state
        varchar postal_code
        char country_code
        boolean is_primary
        timestamp created_at
    }

    PATIENT_CONTACT {
        uuid id PK
        uuid patient_id FK
        varchar contact_type
        varchar value
        boolean is_primary
        timestamp verified_at
        timestamp created_at
    }

    EMERGENCY_CONTACT {
        uuid id PK
        uuid patient_id FK
        varchar name
        varchar relationship
        varchar phone
        varchar email
        text address
        boolean is_primary
        timestamp created_at
    }

    PATIENT_IDENTIFIER {
        uuid id PK
        uuid patient_id FK
        varchar identifier_type
        varchar value
        varchar issuing_authority
        timestamp created_at
    }

    PATIENT_MERGE {
        uuid id PK
        uuid source_patient_id FK
        uuid target_patient_id FK
        text reason
        timestamp merged_at
        uuid merged_by
    }
