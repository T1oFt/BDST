import hashlib
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook


def upsert_parties_scd2(
    parties_data: List[Dict[str, str]], 
    pghook: PostgresHook, 
    loaded_at: datetime
) -> List[int]:
    conn = pghook.get_conn()
    cursor = conn.cursor()
    result_ids = {}
    
    try:
        for party in parties_data:
            name = party['name']
            party_type = party['type']
            
            new_hashdiff = hashlib.md5(
                f"{name}{party_type}".encode('utf-8')
            ).hexdigest()
            
            cursor.execute("""
                SELECT id, hashdiff, valid_to 
                FROM dds.parties 
                WHERE name = %s AND is_current = true
            """, (name,))
            
            existing = cursor.fetchone()

            if existing and existing[1] == new_hashdiff:
                result_ids[name] = existing[0]
                continue
            
            try:
                if existing:
                    existing_id = existing[0]
                    cursor.execute(
                        """
                        UPDATE dds.parties
                        SET valid_to = %s,
                            is_current = false
                        WHERE id = %s
                        """,
                        (loaded_at, existing_id),
                    )

                future_valid_to = loaded_at + timedelta(days=365 * 100)
                cursor.execute(
                    """
                    INSERT INTO dds.parties (
                        name, valid_from, valid_to,
                        is_current, type, hashdiff
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (name, loaded_at, future_valid_to, True, party_type, new_hashdiff),
                )
                new_id = cursor.fetchone()[0]

                conn.commit()
                result_ids[name] = new_id

            except Exception as e:
                conn.rollback()
                logging.error("Error during party upsert with name: {name}, exception: {e}")
                if existing_id:
                    result_ids[name] = existing_id
                continue
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Error during parties upsert: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return result_ids
