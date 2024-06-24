CREATE TYPE broker_type AS ENUM ('manual', 'alpaca');

CREATE TABLE broker (
    id INT GENERATED ALWAYS AS IDENTITY,
    author TEXT NOT NULL,
    name TEXT NOT NULL,
    type broker_type NOT NULL,
    credentials JSON,

    UNIQUE (author, name),
    PRIMARY KEY(id)
);

CREATE TABLE portfolio (
    id INT GENERATED ALWAYS AS IDENTITY,
    author TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    broker_id INT NOT NULL,
    name TEXT NOT NULL,
    shortname TEXT NOT NULL,
    module TEXT NOT NULL,
    schedule TEXT NOT NULL,
    start_timestamp TIMESTAMP WITHOUT TIME ZONE,
    last_run_timestamp TIMESTAMP WITHOUT TIME ZONE,
    
    PRIMARY KEY(id),
    UNIQUE (author, name),
    CONSTRAINT fk_broker FOREIGN KEY (broker_id) REFERENCES broker(id)
);

CREATE TYPE run_status AS ENUM ('succeeded', 'failed');

CREATE TABLE portfolio_run (
    id INT GENERATED ALWAYS AS IDENTITY,
    portfolio_id INT NOT NULL,
    status run_status NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    error TEXT,
    notified BOOLEAN NOT NULL,

    PRIMARY KEY(id),
    CONSTRAINT fk_portfolio FOREIGN KEY (portfolio_id) REFERENCES portfolio(id)
);

CREATE TYPE order_status AS ENUM ('open', 'filled', 'unfilled');
CREATE TYPE order_side AS ENUM ('buy', 'sell');

CREATE TABLE portfolio_order (
    id INT GENERATED ALWAYS AS IDENTITY,
    portfolio_id INT NOT NULL,
    run_id INT NOT NULL,
    status order_status NOT NULL,
    ticker TEXT NOT NULL,
    side order_side NOT NULL,
    create_timestamp TIMESTAMP WITHOUT TIME ZONE,
    notional DECIMAL,
    quantity DECIMAL,
    fill_timestamp TIMESTAMP WITHOUT TIME ZONE,
    fill_quantity DECIMAL,
    fill_price DECIMAL,
    fill_fee DECIMAL,
    broker_order_id TEXT,
    notified BOOLEAN NOT NULL,

    PRIMARY KEY(id),
    CONSTRAINT fk_portfolio FOREIGN KEY (portfolio_id) REFERENCES portfolio(id),
    CONSTRAINT fk_portfolio_run FOREIGN KEY (run_id) REFERENCES portfolio_run(id)
);

--                                  +           -            -         +         +        -
CREATE TYPE cash_event AS ENUM ('deposit', 'withdrawal', 'purchase', 'sale', 'dividend', 'fee');

CREATE TABLE portfolio_cash (
    id INT GENERATED ALWAYS AS IDENTITY,
    portfolio_id INT NOT NULL,
    event cash_event NOT NULL,
    event_timestamp TIMESTAMP WITHOUT TIME ZONE,
    amount DECIMAL NOT NULL,
    order_id INT UNIQUE,

    PRIMARY KEY(id),
    CONSTRAINT fk_portfolio FOREIGN KEY (portfolio_id) REFERENCES portfolio(id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES portfolio_order(id)
);

--                                      +         -        +        -           +                 -
CREATE TYPE position_event AS ENUM ('purchase', 'sale', 'borrow', 'loan', 'forward_split', 'reverse_split');

CREATE TABLE portfolio_position (
    id INT GENERATED ALWAYS AS IDENTITY,
    portfolio_id INT NOT NULL,
    event position_event NOT NULL,
    event_timestamp TIMESTAMP WITHOUT TIME ZONE,
    ticker TEXT NOT NULL,
    amount DECIMAL NOT NULL,
    order_id INT UNIQUE,

    PRIMARY KEY(id),
    CONSTRAINT fk_portfolio FOREIGN KEY (portfolio_id) REFERENCES portfolio(id),
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES portfolio_order(id)
);

CREATE OR REPLACE FUNCTION update_cash_and_position()
  RETURNS trigger 
AS
$$
  BEGIN
    IF (NEW.status = 'filled') AND (NEW.side = 'buy') THEN
        INSERT INTO portfolio_position (event, event_timestamp, portfolio_id, ticker, amount, order_id) values ('purchase', NEW.fill_timestamp, NEW.portfolio_id, NEW.ticker, NEW.fill_quantity, NEW.id) ON CONFLICT (order_id) DO UPDATE
            SET event = EXCLUDED.event,
                event_timestamp = EXCLUDED.event_timestamp,
                portfolio_id = EXCLUDED.portfolio_id,
                ticker = EXCLUDED.ticker,
                amount = EXCLUDED.amount;
        INSERT INTO portfolio_cash (event, event_timestamp, portfolio_id, amount, order_id) values ('purchase', NEW.fill_timestamp, NEW.portfolio_id, -(NEW.fill_quantity * NEW.fill_price + NEW.fill_fee), NEW.id) ON CONFLICT (order_id) DO UPDATE
            SET event = EXCLUDED.event,
                event_timestamp = EXCLUDED.event_timestamp,
                portfolio_id = EXCLUDED.portfolio_id,
                amount = EXCLUDED.amount;
    ELSIF (NEW.status = 'filled') AND (NEW.side = 'sell') THEN
        INSERT INTO portfolio_position (event, event_timestamp, portfolio_id, ticker, amount, order_id) values ('sale', NEW.fill_timestamp, NEW.portfolio_id, NEW.ticker, -NEW.fill_quantity, NEW.id) ON CONFLICT (order_id) DO UPDATE
            SET event = EXCLUDED.event,
                event_timestamp = EXCLUDED.event_timestamp,
                portfolio_id = EXCLUDED.portfolio_id,
                ticker = EXCLUDED.ticker,
                amount = EXCLUDED.amount;
        INSERT INTO portfolio_cash (event, event_timestamp, portfolio_id, amount, order_id) values ('sale', NEW.fill_timestamp, NEW.portfolio_id, NEW.fill_quantity * NEW.fill_price - NEW.fill_fee, NEW.id) ON CONFLICT (order_id) DO UPDATE
            SET event = EXCLUDED.event,
                event_timestamp = EXCLUDED.event_timestamp,
                portfolio_id = EXCLUDED.portfolio_id,
                amount = EXCLUDED.amount;
    END IF;
    RETURN NEW;
  END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER cash_and_position_update AFTER INSERT OR UPDATE ON portfolio_order FOR EACH ROW EXECUTE FUNCTION update_cash_and_position();