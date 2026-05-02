--- CREATE DATABASE DATA_MINING;
--- GO

USE DATA_MINING;
GO

CREATE TABLE applications (
    id INT IDENTITY(1,1) PRIMARY KEY,

    -- ===== FEATURES =====
    status NVARCHAR(10),
    duration INT,
    credit_history NVARCHAR(10),
    purpose NVARCHAR(10),
    credit_amount INT,
    savings NVARCHAR(10),
    employment NVARCHAR(10),
    installment_rate INT,
    personal_status NVARCHAR(10),
    other_debtors NVARCHAR(10),
    residence_since INT,
    property NVARCHAR(10),
    age INT,
    other_installment_plans NVARCHAR(10),
    housing NVARCHAR(10),
    existing_credits INT,
    job NVARCHAR(10),
    people_liable INT,
    telephone NVARCHAR(10),
    foreign_worker NVARCHAR(10),
    target INT NULL, -- 1 good / 0 bad

    -- ===== SYSTEM =====
    data_source NVARCHAR(20) NOT NULL DEFAULT 'SYSTEM', -- UCI | SYSTEM

    -- ===== MODEL OUTPUT =====
    model_prediction INT NULL,
    model_probability FLOAT NULL,

    -- ===== HUMAN =====
    human_decision INT NULL, -- 1 approve / 0 reject

    created_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE outcomes (
    id INT IDENTITY(1,1) PRIMARY KEY,

    application_id INT NOT NULL,

    due_date DATE,
    paid BIT DEFAULT 0,
    paid_date DATE NULL,

    actual_outcome INT NULL, -- 1 good / 0 bad

    updated_at DATETIME DEFAULT GETDATE(),

    CONSTRAINT fk_application
        FOREIGN KEY (application_id)
        REFERENCES applications(id)
        ON DELETE CASCADE
);

ALTER TABLE applications
ADD CONSTRAINT chk_target CHECK (target IN (0,1) OR target IS NULL);

ALTER TABLE applications
ADD CONSTRAINT chk_prediction CHECK (model_prediction IN (0,1) OR model_prediction IS NULL);

ALTER TABLE applications
ADD CONSTRAINT chk_decision CHECK (human_decision IN (0,1) OR human_decision IS NULL);

ALTER TABLE outcomes
ADD CONSTRAINT chk_outcome CHECK (actual_outcome IN (0,1) OR actual_outcome IS NULL);

ALTER TABLE applications
ADD CONSTRAINT chk_probability
CHECK (model_probability >= 0 AND model_probability <= 1 OR model_probability IS NULL);

ALTER TABLE applications
ADD CONSTRAINT chk_source
CHECK (data_source IN ('UCI', 'SYSTEM'));

-- join nhanh khi retrain
CREATE INDEX idx_outcomes_app_id
ON outcomes(application_id);

-- filter theo outcome
CREATE INDEX idx_outcomes_actual
ON outcomes(actual_outcome);

-- filter theo nguồn data
CREATE INDEX idx_applications_source
ON applications(data_source);

GO