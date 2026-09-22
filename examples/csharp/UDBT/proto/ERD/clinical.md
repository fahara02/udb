erDiagram

    ENCOUNTER ||--o{ CHIEF_COMPLAINT : has
    ENCOUNTER ||--o{ SYMPTOM : has
    ENCOUNTER ||--o{ HISTORY : contains
    ENCOUNTER ||--o{ EXAMINATION : contains
    ENCOUNTER ||--o{ VITAL_SIGN : records
    ENCOUNTER ||--o{ CLINICAL_NOTE : contains
    ENCOUNTER ||--o{ ASSESSMENT : has
    ENCOUNTER ||--o{ DIAGNOSIS : contains
    ENCOUNTER ||--o{ CLINICAL_OBSERVATION : records
    ENCOUNTER ||--o{ CARE_PLAN : creates
    ENCOUNTER ||--o{ FOLLOW_UP_PLAN : creates

    PATIENT_PROBLEM ||--o{ ENCOUNTER_PROBLEM : referenced_by
    ENCOUNTER ||--o{ ENCOUNTER_PROBLEM : addresses

    PATIENT_ALLERGY ||--o{ ENCOUNTER : relevant_to


    ENCOUNTER {
        uuid id PK
        uuid patient_id
        uuid provider_id
        uuid organization_id
        uuid location_id
        uuid appointment_id
        string encounter_type
        string status
        timestamp started_at
        timestamp ended_at
        timestamp created_at
    }

    CHIEF_COMPLAINT {
        uuid id PK
        uuid encounter_id FK
        text complaint
        timestamp recorded_at
    }

    SYMPTOM {
        uuid id PK
        uuid encounter_id FK
        string name
        string severity
        string duration
        text description
    }

    HISTORY {
        uuid id PK
        uuid encounter_id FK
        string history_type
        text content
    }

    EXAMINATION {
        uuid id PK
        uuid encounter_id FK
        string body_system
        string finding
        text description
    }

    VITAL_SIGN {
        uuid id PK
        uuid encounter_id FK
        string type
        decimal value
        string unit
        timestamp measured_at
    }

    CLINICAL_NOTE {
        uuid id PK
        uuid encounter_id FK
        string note_type
        text content
        uuid author_id
        timestamp created_at
    }

    ASSESSMENT {
        uuid id PK
        uuid encounter_id FK
        text assessment
        timestamp created_at
    }

    DIAGNOSIS {
        uuid id PK
        uuid encounter_id FK
        string code
        string coding_system
        string description
        string diagnosis_type
        string status
    }

    PATIENT_PROBLEM {
        uuid id PK
        uuid patient_id
        string code
        string coding_system
        string description
        string status
        timestamp onset_date
    }

    ENCOUNTER_PROBLEM {
        uuid id PK
        uuid encounter_id FK
        uuid problem_id FK
        string status
    }

    PATIENT_ALLERGY {
        uuid id PK
        uuid patient_id
        string substance
        string reaction
        string severity
        string status
    }

    CLINICAL_OBSERVATION {
        uuid id PK
        uuid encounter_id FK
        string observation_type
        string code
        string value
        string unit
        string interpretation
        timestamp observed_at
    }

    CARE_PLAN {
        uuid id PK
        uuid encounter_id FK
        string title
        text description
        string status
        timestamp start_date
        timestamp end_date
    }

    FOLLOW_UP_PLAN {
        uuid id PK
        uuid encounter_id FK
        string instruction
        string timeframe
        string status
    }
