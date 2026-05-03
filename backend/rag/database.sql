CREATE TABLE analysis (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    name NVARCHAR(255) NOT NULL UNIQUE,
    status NVARCHAR(50) NOT NULL 
        CONSTRAINT chk_analysis_status CHECK (status IN ('pending', 'processing', 'done', 'error'))
        DEFAULT 'pending',
    final_synthesis_markdown NVARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE documents (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    analysis_id UNIQUEIDENTIFIER NOT NULL
        REFERENCES analysis(id) ON DELETE CASCADE,
    file_name NVARCHAR(255) NOT NULL,
    pdf_url NVARCHAR(MAX) NOT NULL,
    operation_id NVARCHAR(255) NULL,
    status NVARCHAR(50) NOT NULL 
        CONSTRAINT chk_documents_status CHECK (status IN ('pending', 'processing', 'done', 'error'))
        DEFAULT 'processing',
    total_risk_score FLOAT DEFAULT 0,
    ranking_position INT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE pdf_chunks (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    document_id UNIQUEIDENTIFIER NOT NULL
        REFERENCES documents(id) ON DELETE CASCADE,
    page_number INT NOT NULL,
    chunk_index INT NOT NULL,
    text NVARCHAR(MAX) NOT NULL,
    embedding VARBINARY(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE risk_vectors (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    risk_code NVARCHAR(255) NOT NULL,
    text NVARCHAR(MAX) NOT NULL,
    risk_weight INT NOT NULL DEFAULT 5,
    embedding VARBINARY(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE risk_analysis_results (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    document_id UNIQUEIDENTIFIER NOT NULL
        REFERENCES documents(id) ON DELETE CASCADE,
    risk_id UNIQUEIDENTIFIER NOT NULL
        REFERENCES risk_vectors(id),
    coverage NVARCHAR(50) NOT NULL CHECK (coverage IN ('full', 'partial', 'none')),
    explanation NVARCHAR(MAX) NOT NULL,
    matched_chunk_ids NVARCHAR(MAX) NOT NULL, -- JSON array of GUIDs
    created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
