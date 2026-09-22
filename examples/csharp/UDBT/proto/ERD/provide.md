erDiagram

    PROVIDER ||--o{ PROVIDER_SPECIALTY : has
    SPECIALTY ||--o{ PROVIDER_SPECIALTY : assigned_to

    SPECIALTY ||--o{ SPECIALTY : "parent of"

    PROVIDER ||--o{ CREDENTIAL : has
    PROVIDER ||--o{ LICENSE : has

    PROVIDER ||--o{ PROVIDER_AFFILIATION : works_at


    PROVIDER {
        uuid id PK
        varchar provider_code UK
        varchar first_name
        varchar middle_name
        varchar last_name
        varchar display_name
        varchar provider_type
        varchar gender
        date date_of_birth
        varchar status
        timestamp created_at
        timestamp updated_at
    }

    SPECIALTY {
        uuid id PK
        varchar code UK
        varchar name
        text description
        uuid parent_specialty_id FK
        varchar status
    }

    PROVIDER_SPECIALTY {
        uuid provider_id PK,FK
        uuid specialty_id PK,FK
        boolean is_primary
        integer years_of_experience
    }

    CREDENTIAL {
        uuid id PK
        uuid provider_id FK
        varchar credential_type
        varchar name
        varchar issuing_organization
        varchar credential_number
        date issued_date
        date expiry_date
        varchar status
        timestamp created_at
    }

    LICENSE {
        uuid id PK
        uuid provider_id FK
        varchar license_type
        varchar license_number
        varchar issuing_authority
        varchar jurisdiction
        date issued_date
        date expiry_date
        varchar status
        timestamp created_at
    }

    PROVIDER_AFFILIATION {
        uuid id PK
        uuid provider_id FK
        uuid organization_id
        uuid organization_unit_id
        varchar role
        varchar employment_type
        date start_date
        date end_date
        varchar status
        timestamp created_at
    }
