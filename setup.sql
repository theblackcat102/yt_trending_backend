CREATE OR REPLACE FUNCTION jsonb_message_to_string( jsondata jsonb, out string text )
AS $func$
  BEGIN
    SELECT INTO string
      string_agg(d->>'tags', ' ')
    FROM jsonb_array_elements(jsondata) AS d;
    RETURN;
  END;
$func$ LANGUAGE plpgsql
IMMUTABLE;


CREATE AGGREGATE tsvector_agg (tsvector) (
  SFUNC = tsvector_concat,
  STYPE = tsvector
);

CREATE OR REPLACE FUNCTION jsonb_message_to_tsvector( jsondata jsonb, out tsv tsvector )
AS $func$
  BEGIN
    SELECT INTO tsv
      tsvector_agg(to_tsvector(d->>'tag'))
    FROM jsonb_array_elements(jsondata) AS d;
    RETURN;
  END;
$func$ LANGUAGE plpgsql
IMMUTABLE;


CREATE INDEX ON dailytrend
  USING gin (jsonb_message_to_tsvector(jsondata));

SELECT jsonb_message_to_tsvector(metrics) @@ '韓國瑜' FROM dailytrend;
