erDiagram

    MEDICATION ||--o{ MEDICATION_INGREDIENT : contains
    INGREDIENT ||--o{ MEDICATION_INGREDIENT : used_in

    MEDICATION ||--o{ MEDICATION_STRENGTH : has
    MEDICATION ||--o{ MEDICATION_FORM : available_as
    MEDICATION ||--o{ MEDICATION_ROUTE : supports
    MEDICATION ||--o{ MEDICATION_CLASSIFICATION : classified_as

    CLASSIFICATION ||--o{ MEDICATION_CLASSIFICATION : classifies

    PATIENT_MEDICATION ||--o{ MEDICATION_HISTORY : has

    MEDICATION ||--o{ PATIENT_MEDICATION : used_by

    MEDICATION ||--o{ DRUG_INTERACTION : medication_a
    MEDICATION ||--o{ DRUG_INTERACTION : medication_b

    ALLERGY_RULE ||--o{ ALLERGY_SAFETY_RULE : defines
    MEDICATION ||--o{ ALLERGY_SAFETY_RULE : checked_against


    MEDICATION {
        uuid id PK
        string code
        string name
        string generic_name
        string brand_name
        string status
    }

    INGREDIENT {
        uuid id PK
        string code
        string name
        string status
    }

    MEDICATION_INGREDIENT {
        uuid medication_id PK,FK
        uuid ingredient_id PK,FK
        string amount
        string unit
        string strength
    }

    MEDICATION_STRENGTH {
        uuid id PK
        uuid medication_id FK
        decimal value
        string unit
    }

    MEDICATION_FORM {
        uuid id PK
        uuid medication_id FK
        string form
    }

    MEDICATION_ROUTE {
        uuid id PK
        uuid medication_id FK
        string route
    }

    CLASSIFICATION {
        uuid id PK
        string system
        string code
        string name
    }

    MEDICATION_CLASSIFICATION {
        uuid medication_id PK,FK
        uuid classification_id PK,FK
    }

    PATIENT_MEDICATION {
        uuid id PK
        uuid patient_id
        uuid medication_id
        string status
        string dose
        string frequency
        string route
        string indication
        date start_date
        date end_date
        uuid source_reference_id
        timestamp created_at
    }

    MEDICATION_HISTORY {
        uuid id PK
        uuid patient_medication_id FK
        string event_type
        string old_status
        string new_status
        string reason
        timestamp event_at
    }

    DRUG_INTERACTION {
        uuid id PK
        uuid medication_a_id FK
        uuid medication_b_id FK
        string severity
        text description
        text recommendation
    }

    ALLERGY_RULE {
        uuid id PK
        string allergen
        string reaction
        string severity
    }

    ALLERGY_SAFETY_RULE {
        uuid id PK
        uuid allergy_rule_id FK
        uuid medication_id FK
        string severity
        text recommendation
    }
