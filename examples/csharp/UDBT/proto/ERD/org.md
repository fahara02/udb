erDiagram

    ORGANIZATION ||--o{ LOCATION : has
    ORGANIZATION ||--o{ ORGANIZATION_UNIT : contains

    ORGANIZATION_UNIT ||--o{ ORGANIZATION_UNIT : "parent of"
    ORGANIZATION_UNIT ||--o{ ROOM : contains

    LOCATION ||--o{ ORGANIZATION_UNIT : serves
    LOCATION ||--o{ ROOM : serves

    ROOM ||--o{ BED : contains

    ORGANIZATION ||--o{ OPERATING_HOURS : defines
    ORGANIZATION_UNIT ||--o{ OPERATING_HOURS : defines


    ORGANIZATION {
        uuid id PK
        varchar code UK
        varchar name
        varchar organization_type
        varchar status
        varchar phone
        varchar email
        timestamp created_at
        timestamp updated_at
    }

    LOCATION {
        uuid id PK
        uuid organization_id FK
        varchar name
        varchar address_line_1
        varchar address_line_2
        varchar city
        varchar state
        varchar postal_code
        char country_code
        decimal latitude
        decimal longitude
        varchar timezone
        timestamp created_at
        timestamp updated_at
    }

    ORGANIZATION_UNIT {
        uuid id PK
        uuid organization_id FK
        uuid parent_unit_id FK
        uuid location_id FK
        varchar code
        varchar name
        varchar unit_type
        varchar status
        timestamp created_at
        timestamp updated_at
    }

    ROOM {
        uuid id PK
        uuid unit_id FK
        uuid location_id FK
        varchar room_number
        varchar room_type
        varchar status
        timestamp created_at
        timestamp updated_at
    }

    BED {
        uuid id PK
        uuid room_id FK
        varchar bed_number
        varchar bed_type
        varchar status
        timestamp created_at
        timestamp updated_at
    }

    OPERATING_HOURS {
        uuid id PK
        uuid organization_id FK
        uuid unit_id FK
        smallint day_of_week
        time opens_at
        time closes_at
        boolean is_closed
    }
