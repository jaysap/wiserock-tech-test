-- #############################################################################
-- ## Analytics Engineer Technical Assessment (Part 2b: Wise Rock Analytics)  ##
-- #############################################################################

-- Indexes for performance optimization
CREATE INDEX ON stg.wiserock_note(event_uuid);
CREATE INDEX ON stg.wiserock_note(note_timestamp);
CREATE INDEX ON stg.wiserock_note(user_id);
CREATE INDEX ON stg.wiserock_user(user_id);

-- #############################################################################
-- ## Section 1: View Creation
-- #############################################################################

-- =============================================================================
-- View 1: note_relationships
-- This view analyzes each note to determine which users were explicitly tagged
-- (e.g., "@sergio") and which were implicitly tagged (i.e., they participated
-- in the conversation earlier but were not the author or explicitly mentioned).
-- =============================================================================

-- =============================================================================
-- View 1: note_relationships
-- Purpose: This view analyzes each note to determine which users were 
--          explicitly tagged and which were implicitly tagged
-- Sources:
--     wiserock_note 
--         Columns: note_id, event_uuid, note_timestamp, note_text, user_id
--     wiserock_user
--         Columns: handle
-- =============================================================================

CREATE OR REPLACE VIEW analytics.note_relationships AS
WITH note_base AS (
    -- First, join notes with their authors to get user handles
    SELECT
        n.note_id,
        n.event_uuid,
        n.note_timestamp,
        n.note_text,
        n.user_id AS author_user_id,
        u.handle AS author_handle
    FROM
        raw_v3.wiserock_note n
    JOIN
        raw_v3.wiserock_user u ON n.user_id = u.user_id
),
explicit_tags AS (
    -- Identify all explicit "@" mentions in each note
    SELECT
        nb.note_id,
        COALESCE(
            ARRAY_AGG(DISTINCT(LOWER(matches[1]))) FILTER (WHERE matches[1] IS NOT NULL),
            '{}'::TEXT[]
        ) AS explicit_tagged_handles
    FROM
        note_base nb,
        regexp_matches(nb.note_text, '@(\w+)', 'g') AS matches
    GROUP BY
        nb.note_id
),
conversation_participants AS (
    -- For each note, determine the set of all users who had participated
    -- in the conversation up to that point
    SELECT
        note_id,
        -- Use a window function to aggregate all user handles within the same event,
        -- ordered by time. This creates an array of historical participants
        ARRAY_AGG(author_handle) OVER (
            PARTITION BY event_uuid
            ORDER BY note_timestamp::timestamp -- Cast to timestamp for correct chronological ordering
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS past_participant_handles
    FROM
        note_base
)
-- Final SELECT to assemble the view.
SELECT
    nb.note_id,
    nb.event_uuid,
    -- Cast the text timestamp to a proper TIMESTAMP data type here
    nb.note_timestamp::timestamp AS note_timestamp,
    nb.author_user_id,
    nb.author_handle,
    COALESCE(et.explicit_tagged_handles, '{}'::TEXT[]) AS explicit_tagged_users,
    -- Calculate implicit tags using array manipulation
    (
        SELECT
            ARRAY(
                SELECT UNNEST(COALESCE(cp.past_participant_handles, '{}'::TEXT[]))
                EXCEPT
                SELECT UNNEST(COALESCE(et.explicit_tagged_handles, '{}'::TEXT[]))
                EXCEPT
                SELECT nb.author_handle
            )
    ) AS implicit_tagged_users
FROM
    note_base nb
LEFT JOIN
    explicit_tags et ON nb.note_id = et.note_id
LEFT JOIN
    conversation_participants cp ON nb.note_id = cp.note_id;


-- #############################################################################
-- ## Section 2: Analytical Queries
-- #############################################################################

-- =============================================================================
-- Query 1: Which conversation included the most explicitly tagged users?
-- =============================================================================
SELECT
    event_uuid,
    SUM(CARDINALITY(explicit_tagged_users)) AS total_explicit_tags_in_conversation
FROM
    analytics.note_relationships
WHERE
    event_uuid IS NOT NULL
GROUP BY
    event_uuid
ORDER BY
    total_explicit_tags_in_conversation DESC
LIMIT 1;

-- =============================================================================
-- Query 2: User Summary Metrics
-- This complex query calculates several engagement metrics for each user
-- =============================================================================
WITH user_base AS (
    -- Start with a distinct list of all users.
    SELECT user_id, handle FROM raw_v3.wiserock_user
),
explicitly_tagged_counts AS (
    -- Count how many times each user was explicitly tagged
    SELECT
        u.user_id,
        COUNT(*) AS times_explicitly_tagged
    FROM
        analytics.note_relationships nr,
        UNNEST(nr.explicit_tagged_users) AS tagged_handle
    JOIN
        raw_v3.wiserock_user u ON u.handle = tagged_handle
    WHERE
        nr.event_uuid IS NOT NULL
    GROUP BY
        u.user_id
),
implicitly_tagged_counts AS (
    -- Count how many times each user was implicitly tagged
    SELECT
        u.user_id,
        COUNT(*) AS times_implicitly_tagged
    FROM
        analytics.note_relationships nr,
        UNNEST(nr.implicit_tagged_users) AS tagged_handle
    JOIN
        raw_v3.wiserock_user u ON u.handle = tagged_handle
    WHERE
        nr.event_uuid IS NOT NULL
    GROUP BY
        u.user_id
),
tagging_pairs AS (
    -- Create a record for every time a user tags another user
    SELECT
        nr.author_user_id AS tagger_user_id,
        u.user_id AS tagged_user_id
    FROM
        analytics.note_relationships nr,
        UNNEST(nr.explicit_tagged_users) AS tagged_handle
    JOIN
        raw_v3.wiserock_user u ON u.handle = tagged_handle
    WHERE
        nr.event_uuid IS NOT NULL
),
top_tagger_for_user AS (
    -- For each user, find who tagged them the most
    SELECT DISTINCT ON (tagged_user_id)
        tagged_user_id,
        tagger_user_id AS top_tagger_user_id
    FROM tagging_pairs
    GROUP BY tagged_user_id, tagger_user_id
    ORDER BY tagged_user_id, COUNT(*) DESC
),
top_tagged_by_user AS (
    -- For each user, find who they tagged the most
    SELECT DISTINCT ON (tagger_user_id)
        tagger_user_id,
        tagged_user_id AS top_tagged_user_id
    FROM tagging_pairs
    GROUP BY tagger_user_id, tagged_user_id
    ORDER BY tagger_user_id, COUNT(*) DESC
),
all_tags_and_responses AS (
    -- For each explicit tag, find the timestamp of the tagged user's
    -- first subsequent message in the same conversation
    SELECT
        nr.note_id,
        u_tagged.user_id AS tagged_user_id,
        (
            SELECT
                MIN(nr2.note_timestamp)
            FROM
                analytics.note_relationships nr2
            WHERE
                nr2.event_uuid = nr.event_uuid
                AND nr2.author_user_id = u_tagged.user_id
                AND nr2.note_timestamp > nr.note_timestamp
        ) AS response_timestamp,
        nr.note_timestamp AS tag_timestamp
    FROM
        analytics.note_relationships nr,
        UNNEST(nr.explicit_tagged_users) AS tagged_handle
    JOIN
        raw_v3.wiserock_user u_tagged ON tagged_handle = u_tagged.handle
    WHERE
        nr.event_uuid IS NOT NULL
),
avg_response_times AS (
    -- Calculate the average response time from the intervals found above
    SELECT
        tagged_user_id,
        AVG(response_timestamp - tag_timestamp) AS avg_response_time
    FROM
        all_tags_and_responses
    WHERE
        response_timestamp IS NOT NULL -- Only average actual responses
    GROUP BY
        tagged_user_id
),
no_response_counts AS (
    -- Count the instances where a response timestamp was not found
    SELECT
        tagged_user_id,
        COUNT(*) AS times_tagged_with_no_response
    FROM
        all_tags_and_responses
    WHERE
        response_timestamp IS NULL -- This indicates the user was tagged but never responded
    GROUP BY
        tagged_user_id
)
-- Final assembly of the user summary table
SELECT
    ub.user_id,
    ub.handle,
    COALESCE(etc.times_explicitly_tagged, 0) AS total_times_explicitly_tagged,
    COALESCE(itc.times_implicitly_tagged, 0) AS total_times_implicitly_tagged,
    ttfu.top_tagger_user_id,
    ttbu.top_tagged_user_id,
    art.avg_response_time,
    COALESCE(nrc.times_tagged_with_no_response, 0) AS count_of_times_tagged_but_never_responded
FROM
    user_base ub
LEFT JOIN
    explicitly_tagged_counts etc ON ub.user_id = etc.user_id
LEFT JOIN
    implicitly_tagged_counts itc ON ub.user_id = itc.user_id
LEFT JOIN
    top_tagger_for_user ttfu ON ub.user_id = ttfu.tagged_user_id
LEFT JOIN
    top_tagged_by_user ttbu ON ub.user_id = ttbu.tagger_user_id
LEFT JOIN
    avg_response_times art ON ub.user_id = art.tagged_user_id
LEFT JOIN
    no_response_counts nrc ON ub.user_id = nrc.tagged_user_id
ORDER BY
    ub.user_id;



