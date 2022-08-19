import psycopg2
import config_manager as config


def create_tables():
    commands = (
        """
       CREATE TABLE "candidates" (
      "candidate_id" INT GENERATED ALWAYS AS IDENTITY,
      "first_name" VARCHAR,
      "middle_name" VARCHAR,
      "last_name" VARCHAR,
      "gender" VARCHAR,
      "dob" TEXT,
      "email" VARCHAR,
      "city" VARCHAR,
      "address" VARCHAR,
      "postcode" VARCHAR,
      "phone_number" VARCHAR,
      PRIMARY KEY ("candidate_id"));
    """,

        """
        CREATE TABLE "stream" (
        "stream_id" INT GENERATED ALWAYS AS IDENTITY,
        "course_name" VARCHAR,
        "course_start_date" TEXT,
        PRIMARY KEY ("stream_id")
        );
        """,

        """
        CREATE TABLE "stream_junction" (
        "candidate_id" INT,
        "stream_id" INT,
        CONSTRAINT "FK_stream_junction.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id"),
        CONSTRAINT "FK_stream_junction.stream_id"
        FOREIGN KEY ("stream_id")
        REFERENCES "stream"("stream_id")
        );""",
        """
        CREATE TABLE "trainers" (
        "trainer_id" INT GENERATED ALWAYS AS IDENTITY,
        "stream_id" INT,
        "name" VARCHAR,
        PRIMARY KEY ("trainer_id"),
	    CONSTRAINT "FK_trainers.stream_id"
        FOREIGN KEY ("stream_id")
        REFERENCES "stream"("stream_id")
        );""",
        """
        CREATE TABLE "academy" (
        "academy_id" INT,
        "trainer_id" INT,
        "location" VARCHAR,
        PRIMARY KEY ("academy_id"),
        CONSTRAINT "FK_academy.trainer_id"
        FOREIGN KEY ("trainer_id")
        REFERENCES "trainers"("trainer_id")
        );
        """,

        """
        CREATE TABLE "behaviours" (
        "behaviours_id" INT GENERATED ALWAYS AS IDENTITY,
        "behaviour" VARCHAR,
        PRIMARY KEY ("behaviours_id")
        );""",

"""
        CREATE TABLE "behaviour_week_junction" (
        "candidate_id" INT,
        "week_no" INT,
        "behaviour_id" INT,
        "score" INT,
        CONSTRAINT "FK_behaviour_week_junction.behaviour_id"
        FOREIGN KEY ("behaviour_id")
        REFERENCES "behaviours"("behaviours_id"),
        CONSTRAINT "FK_behaviour_week_junction.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id")
        );""",

        """
        CREATE TABLE "uni" (
        "uni_id" INT GENERATED ALWAYS AS IDENTITY,
        "uni_degree" VARCHAR,
        PRIMARY KEY ("uni_id"));""",

        """
        CREATE TABLE "uni_junction" (
        "candidate_id" INT,
        "uni_id" INT,
        CONSTRAINT "FK_uni_junction.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id"),
        CONSTRAINT "FK_uni_junction.uni_id"
        FOREIGN KEY ("uni_id")
        REFERENCES "uni"("uni_id")
        );""",

        """
        CREATE TABLE "sparta_day_results" (
        "candidate_id" INT,
        "scores_id" INT GENERATED ALWAYS AS IDENTITY,
        "psycho_score" FLOAT,
        "presentation_score" FLOAT,
        "sparta_day_location" VARCHAR,
        "sparta_day_date" TEXT,
        PRIMARY KEY ("scores_id"),
        CONSTRAINT "FK_sparta_day_results.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id"));""",

        """
        CREATE TABLE "recruited" (
        "recruiter_id" INT GENERATED ALWAYS AS IDENTITY,
        "candidate_id" INT,
        "invite_date" TEXT,
        "name" VARCHAR,
        PRIMARY KEY ("recruiter_id"),
        CONSTRAINT "FK_recruited.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id")
        );""",

        """
        CREATE TABLE "general_information" (
        "candidate_id" INT,
        "geo_flex" VARCHAR,
        "financial_support_self" VARCHAR,
        "result" VARCHAR,
        "course_interest" VARCHAR,
        "self_development" VARCHAR,
        CONSTRAINT "FK_general_information.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id")
        );""",

        """
        CREATE TABLE "tech" (
        "tech_id" INT GENERATED ALWAYS AS IDENTITY,
        "technology" VARCHAR,
        PRIMARY KEY ("tech_id")
        );""",

        """
        CREATE TABLE "tech_score_junction" (
        "candidate_id " INT,
        "tech_id" INT,
        "score" VARCHAR,
        CONSTRAINT "FK_tech_score_junction.tech_id"
        FOREIGN KEY ("tech_id")
        REFERENCES "tech"("tech_id"),
        CONSTRAINT "FK_tech_score_junction.candidate_id "
        FOREIGN KEY ("candidate_id ")
        REFERENCES "candidates"("candidate_id")
        );""",

        """
        CREATE TABLE "weakness" (
        "weakness_id" INT GENERATED ALWAYS AS IDENTITY,
        "weakness" VARCHAR,
        PRIMARY KEY ("weakness_id")
        );""",

        """
        CREATE TABLE "weakness_junction" (
        "candidate_id" INT,
        "weakness_id" INT,
        CONSTRAINT "FK_weakness_junction.weakness_id"
        FOREIGN KEY ("weakness_id")
        REFERENCES "weakness"("weakness_id"),
        CONSTRAINT "FK_weakness_junction.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id")
        );""",

        """
        CREATE TABLE "strengths" (
        "strength_id" INT  GENERATED ALWAYS AS IDENTITY,
        "strength" VARCHAR,
        PRIMARY KEY ("strength_id")
        );""",

        """
        CREATE TABLE "strength_junction" (
        "candidate_id" INT,
        "strength_id" INT,
        CONSTRAINT "FK_strength_junction.strength_id"
        FOREIGN KEY ("strength_id")
        REFERENCES "strengths"("strength_id"),
        CONSTRAINT "FK_strength_junction.candidate_id"
        FOREIGN KEY ("candidate_id")
        REFERENCES "candidates"("candidate_id")
        );"""



        )

    connection = None
    try:
        # Connecting to the Sparta database on the Postgres server
        connection = psycopg2.connect(database=config.SPARTA_DB,
                                      user=config.DB_USER,
                                      password=config.DB_PASSWORD,
                                      host=config.HOST,
                                      port=config.PORT)

        connection.autocommit = True
        cursor = connection.cursor()
        # Creating table one by one
        for command in commands:
            cursor.execute(command)
        # Closing the connection with the database server
        cursor.close()


    except Exception as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


create_tables()
