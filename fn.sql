CREATE FUNCTION to_tsvector_multilang(text) RETURNS tsvector AS $$
SELECT to_tsvector('english', $1) || 
       to_tsvector('simple', array_to_string(
            to_chinese_bigram(
                lower($1)
            ), ' '::text
        )) || 
       to_tsvector('french', $1) || 
       to_tsvector('simple', $1)
$$ LANGUAGE sql IMMUTABLE;



CREATE OR REPLACE FUNCTION
to_chinese_bigram (input text)
RETURNS text[]
AS
$$
    DECLARE
        retVal text[];
        chineseVal text[];
        inputVal text[] := Array(select regexp_matches(input, '([\u4e00-\u9fa5]+)|([-a-zA-Z]+)', 'g'));
    BEGIN
        IF array_length(inputVal, 1) > 0 THEN
            FOR i IN 1 .. ARRAY_UPPER(inputVal, 1)
            LOOP
                IF inputVal[i][1] IS NULL THEN
                    IF inputVal[i][2] != '-' THEN
                        retVal = ARRAY_APPEND(retVal, inputVal[i][2]);
                    END IF;
                ELSE
                    chineseVal = REGEXP_SPLIT_TO_ARRAY(inputVal[i][1], '');
                    IF array_length(chineseVal, 1) > 1 THEN
                        FOR j IN 2 .. ARRAY_UPPER(chineseVal, 1)
                        LOOP
                            retVal = ARRAY_APPEND(retVal, chineseVal[j - 1] || chineseVal[j]);
                        END LOOP;
                    END IF;
                END IF;
            END LOOP;
            RETURN retVal;
        ELSE
            RETURN inputVal;
        END IF;
    END;
$$
LANGUAGE plpgsql;