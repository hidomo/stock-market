CREATE TABLE IF NOT EXISTS raw_stock
(
    code               varchar(20),
    name               varchar(20),
    date               varchar(20),
    industry           varchar(450),
    concept            varchar(450),
    area               varchar(450),
    opening            float,
    highest            float,
    lowest             float,
    closing            float,
    post_recovery      float,
    pre_recovery       float,
    quote_change       float,
    volume             float,
    turnover           float,
    hand_turnover_rate float,
    circulation_market float,
    total_market       float,
    daily_limit        float,
    down_limit         float,
    PE_ratio           float,
    market_sales       float,
    market_rate        float,
    pb_ratio           float,
    ma5                float,
    ma10               float,
    ma20               float,
    ma30               float,
    ma60               float,
    ma_cross           varchar(450),
    macd_diff          float,
    macd_dea           float,
    macd_macd          float,
    macd_cross         varchar(450),
    k                  float,
    d                  float,
    j                  float,
    kdj_cross          varchar(450),
    bollinger_mid      float,
    bollinger_up       float,
    bollinger_down     float,
    psy                float,
    psyma              float,
    rsi1               float,
    rsi2               float,
    rsi3               float,
    amp                float,
    volume_ratio       float,
    PRIMARY KEY (code, date)
);

CREATE INDEX IF NOT EXISTS raw_stock_code_idx ON raw_stock (code);
CREATE INDEX IF NOT EXISTS raw_stock_date_idx ON raw_stock (date);


CREATE TABLE IF NOT EXISTS market_index(
    code varchar(20),
    date varchar(20),
    open float,
    close float,
    low float,
    high float,
    volume float,
    money float,
    change float,
    PRIMARY KEY (code,date)
);

CREATE INDEX IF NOT EXISTS index_code_idx ON market_index(code);
CREATE INDEX IF NOT EXISTS index_date_idx ON market_index(date);

CREATE TABLE IF NOT EXISTS mapping
(
    word varchar(450),
    val  integer,
    PRIMARY KEY (word)
);
CREATE INDEX IF NOT EXISTS mapping_val_idx ON mapping (val);


