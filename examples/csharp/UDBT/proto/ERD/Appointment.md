erDiagram

    SCHEDULE ||--o{ AVAILABILITY_RULE : defines
    SCHEDULE ||--o{ APPOINTMENT_SLOT : generates

    PROVIDER_REF ||--o{ SCHEDULE : owns
    RESOURCE_REF ||--o{ SCHEDULE : uses

    APPOINTMENT_SLOT ||--o| APPOINTMENT : booked_by

    APPOINTMENT ||--o{ APPOINTMENT_STATUS_HISTORY : has
    APPOINTMENT ||--o{ QUEUE_ENTRY : creates

    RESOURCE_REF {
        uuid id PK
        string resource_type
        string external_id
    }

    PROVIDER_REF {
        uuid provider_id PK
    }

    SCHEDULE {
        uuid id PK
        uuid provider_id
        uuid resource_id
        uuid organization_id
        uuid location_id
        string name
        string schedule_type
        string status
        timestamp valid_from
        timestamp valid_until
    }

    AVAILABILITY_RULE {
        uuid id PK
        uuid schedule_id FK
        int day_of_week
        time start_time
        time end_time
        int slot_duration_minutes
        int buffer_minutes
        string status
    }

    APPOINTMENT_SLOT {
        uuid id PK
        uuid schedule_id FK
        timestamp start_time
        timestamp end_time
        string status
        string slot_type
    }

    APPOINTMENT {
        uuid id PK
        uuid slot_id FK
        uuid patient_id
        uuid provider_id
        uuid organization_id
        uuid location_id
        uuid resource_id
        string appointment_type
        string reason
        string status
        timestamp booked_at
        timestamp cancelled_at
        timestamp created_at
        timestamp updated_at
    }

    APPOINTMENT_STATUS_HISTORY {
        uuid id PK
        uuid appointment_id FK
        string old_status
        string new_status
        string reason
        uuid changed_by
        timestamp changed_at
    }

    QUEUE_ENTRY {
        uuid id PK
        uuid appointment_id FK
        uuid patient_id
        uuid organization_id
        uuid location_id
        string queue_type
        int queue_number
        string status
        timestamp joined_at
        timestamp called_at
        timestamp completed_at
    }
