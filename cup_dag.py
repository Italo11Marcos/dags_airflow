from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

with DAG('dag_cup',
        start_date = datetime(2021,12,31), 
        schedule_interval='0 0 * * 1',
        catchup=False,
        template_searchpath = '/opt/airflow/sql'
        ) as dag:

    start = EmptyOperator(task_id='start')

    create_table_world_cup = PostgresOperator(
            task_id='create_table_world_cup',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists world_cup (
                winner varchar(50),
                runner varchar(50),
                date timestamp
                )
            """
    )

    with TaskGroup(group_id='cup_table') as cup_table:
        drop_cup_table  = PostgresOperator(
            task_id='drop_cup_table',
            postgres_conn_id='postgres-airflow',
            sql="""DROP TABLE IF EXISTS teams"""
        )

        create_cup_table = PostgresOperator(
            task_id='create_cup_table',
            postgres_conn_id='postgres-airflow',
            sql="""create table teams (
                    id serial,
                    name varchar(50)
                );
                """
        )

        insert_cup_table = PostgresOperator(
            task_id='insert_cup_table',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into teams (name) values ('Alemanha');
                insert into teams (name) values ('Arabia Saudita');
                insert into teams (name) values ('Argentina');
                insert into teams (name) values ('Australia');
                insert into teams (name) values ('Belgica');
                insert into teams (name) values ('Brasil');
                insert into teams (name) values ('Camaroes');
                insert into teams (name) values ('Canada');
                insert into teams (name) values ('Catar');
                insert into teams (name) values ('Coreia do Sul');
                insert into teams (name) values ('Costa Rica');
                insert into teams (name) values ('Croacia');
                insert into teams (name) values ('Dinamarca');
                insert into teams (name) values ('Equador');
                insert into teams (name) values ('Espanha');
                insert into teams (name) values ('Estados Unidos');
                insert into teams (name) values ('França');
                insert into teams (name) values ('Gana');
                insert into teams (name) values ('Holanda');
                insert into teams (name) values ('Inglaterra');
                insert into teams (name) values ('Ira');
                insert into teams (name) values ('Japao');
                insert into teams (name) values ('Marrocos');
                insert into teams (name) values ('Mexico');
                insert into teams (name) values ('Pais de Gales');
                insert into teams (name) values ('Polonia');
                insert into teams (name) values ('Portugal');
                insert into teams (name) values ('Senegal');
                insert into teams (name) values ('Servia');
                insert into teams (name) values ('Suiça');
                insert into teams (name) values ('Tunisia');
                insert into teams (name) values ('Uruguai');
            """
        )

        [drop_cup_table >> create_cup_table >> insert_cup_table]
    
    with TaskGroup(group_id='aux_table_teams') as aux_table_teams:

        create_aux_table_teams = PostgresOperator(
            task_id='create_aux_table_teams',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table if not exists aux_table_teams
            (
                    id integer,
                    name VARCHAR(50)
            );
            """
        )

        truncate_aux_table_teams = PostgresOperator(
            task_id='truncate_aux_table_teams',
            postgres_conn_id='postgres-airflow',
            sql="""truncate table aux_table_teams"""
        )

        insert_aux_table_teams = PostgresOperator(
            task_id='insert_aux_table_teams',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into aux_table_teams
            with random_teams as (
                select * from teams order by random()
            )
            select
            row_number () over(order by id) as ln,
            name
            from (
            select 
            floor(random() * 4095 + 1)::int as id,
            name 
            from random_teams
            ) r
            """
        )

        [create_aux_table_teams >> truncate_aux_table_teams >> insert_aux_table_teams]

    with TaskGroup(group_id='create_tables_groups_A_H') as create_groups_A_H:
        
        drop_tables_groups_A_H = PostgresOperator(
           task_id='drop_tables_groups_A_H',
           postgres_conn_id='postgres-airflow',
           sql="""
            drop table groupa;
            drop table groupb;
            drop table groupc;
            drop table groupd;
            drop table groupe;
            drop table groupf;
            drop table groupg;
            drop table grouph;
           """ 
        )

        create_table_groupA = PostgresOperator(
            task_id='create_table_groupA',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupA 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupB = PostgresOperator(
            task_id='create_table_groupB',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupB 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupC = PostgresOperator(
            task_id='create_table_groupC',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupC 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupD = PostgresOperator(
            task_id='create_table_groupD',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupD 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupE = PostgresOperator(
            task_id='create_table_groupE',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupE 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupF = PostgresOperator(
            task_id='create_table_groupF',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupF 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupG = PostgresOperator(
            task_id='create_table_groupG',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupG 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )

        create_table_groupH = PostgresOperator(
            task_id='create_table_groupH',
            postgres_conn_id='postgres-airflow',
            sql="""
            create table groupH 
            (
                id serial,
                name varchar(50),
                points integer,
                wins integer,
                draws integer,
                loses integer,
                ga integer,
                gc integer
            );
            """
        )
    
        drop_tables_groups_A_H >> [create_table_groupA, create_table_groupB, create_table_groupC, create_table_groupD, create_table_groupE, create_table_groupF, create_table_groupG, create_table_groupH]
    
    with TaskGroup(group_id='create_functions_groups_A_H') as create_functions_A_H:
        create_function_groupA = PostgresOperator(
            task_id='create_function_groupA',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupa()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 1..4 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupA (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupB = PostgresOperator(
            task_id='create_function_groupB',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupb()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 5..8 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupB (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupC = PostgresOperator(
            task_id='create_function_groupC',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupc()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 9..12 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupC (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupD = PostgresOperator(
            task_id='create_function_groupD',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupd()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 13..16 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupD (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupE = PostgresOperator(
            task_id='create_function_groupE',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupe()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 17..20 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupE (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupF = PostgresOperator(
            task_id='create_function_groupF',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupf()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 21..24 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupF (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupG = PostgresOperator(
            task_id='create_function_groupG',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_groupg()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 25..28 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupG (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        create_function_groupH = PostgresOperator(
            task_id='create_function_groupH',
            postgres_conn_id='postgres-airflow',
            sql="""
                create or replace function insert_grouph()
                returns void
                language plpgsql
                as
                $$
                declare nome varchar;
                begin
                    for n in 29..32 loop
                    select name into nome from aux_table_teams where id = n;
                    insert into groupH (name) values (nome);
                    end loop;
                end; 
                $$;
            """
        )

        [create_function_groupA, create_function_groupB, create_function_groupC, create_function_groupD, create_function_groupE, create_function_groupF, create_function_groupG, create_function_groupH]

    with TaskGroup(group_id='call_functions_groups_A_H') as call_functions_A_H:
        call_function_groupA = PostgresOperator(
            task_id='call_function_groupA',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupa()"""
        )

        call_function_groupB = PostgresOperator(
            task_id='call_function_groupB',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupb()"""
        )

        call_function_groupC = PostgresOperator(
            task_id='call_function_groupC',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupc()"""
        )

        call_function_groupD = PostgresOperator(
            task_id='call_function_groupD',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupd()"""
        )

        call_function_groupE = PostgresOperator(
            task_id='call_function_groupE',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupe()"""
        )

        call_function_groupF = PostgresOperator(
            task_id='call_function_groupF',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupf()"""
        )
        
        call_function_groupG = PostgresOperator(
            task_id='call_function_groupG',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_groupg()"""
        )

        call_function_groupH = PostgresOperator(
            task_id='call_function_groupH',
            postgres_conn_id='postgres-airflow',
            sql="""select insert_grouph()"""
        )

        [call_function_groupA, call_function_groupB, call_function_groupC, call_function_groupD, call_function_groupE, call_function_groupF, call_function_groupG, call_function_groupH]

    with TaskGroup(group_id='create_matchs_groups_A_H') as create_matchs_groups_A_H:
        
        drop_table_matchs = PostgresOperator(
            task_id='drop_table_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""drop table if exists matchs"""
        )

        create_table_matchs = PostgresOperator(
                task_id='create_table_matchs',
                postgres_conn_id='postgres-airflow',
                sql="""create table if not exists matchs (
                    gameid serial,
                    teamA varchar(50),
                    teamB varchar(50),
                    goalsA integer,
                    goalsB integer,
                    draw boolean,
                    penaltis boolean,
                    penaltisA integer,
                    penaltisB integer,
                    fase varchar(20)
                )"""
            )
        
        insert_matchs_groupa = PostgresOperator(
            task_id='insert_matchs_groupa',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo A' fase
                    from groupa r1 
                    cross join groupa r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupb = PostgresOperator(
            task_id='insert_matchs_groupb',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo B' fase
                    from groupb r1 
                    cross join groupb r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupc = PostgresOperator(
            task_id='insert_matchs_groupc',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo C' fase
                    from groupc r1 
                    cross join groupc r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupd = PostgresOperator(
            task_id='insert_matchs_groupd',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo D' fase
                    from groupd r1 
                    cross join groupd r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupe = PostgresOperator(
            task_id='insert_matchs_groupe',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo E' fase
                    from groupe r1 
                    cross join groupe r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupf = PostgresOperator(
            task_id='insert_matchs_groupf',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo F' fase
                    from groupf r1 
                    cross join groupf r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_groupg = PostgresOperator(
            task_id='insert_matchs_groupg',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo G' fase
                    from groupg r1 
                    cross join groupg r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        insert_matchs_grouph = PostgresOperator(
            task_id='insert_matchs_grouph',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into matchs (teama, teamb, goalsa, goalsb, draw, penaltis, fase)
                select
                teama,
                teamb,
                goalsa,
                goalsb,
                case
                    when goalsa = goalsb then True
                    else False
                end draw,
                penaltis,
                fase
                from (
                    select 
                    r1.name teama,
                    r2.name teamb,
                    floor(((random() + random())* 4))::int goalsa,
                    floor(((random() + random())* 4))::int goalsb,
                    False penaltis,
                    'grupo H' fase
                    from grouph r1 
                    cross join grouph r2 
                    where r1.id < r2.id
                ) matchs
            """
        )

        drop_table_matchs >> create_table_matchs >> [insert_matchs_groupa, insert_matchs_groupb, insert_matchs_groupc, insert_matchs_groupd, insert_matchs_groupe, insert_matchs_groupf, insert_matchs_groupg, insert_matchs_grouph]
    
    with TaskGroup(group_id='update_table_groups_A_H') as update_table_groups_A_H:

        update_table_groupa = PostgresOperator(
                task_id='update_table_groupa',
                postgres_conn_id='postgres-airflow',
                sql="""update groupa g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupa ga on ga.name = m.teama
                            where fase = 'grupo A'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupa ga on ga.name = m.teamb
                            where fase = 'grupo A'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupb = PostgresOperator(
                task_id='update_table_groupb',
                postgres_conn_id='postgres-airflow',
                sql="""update groupb g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupb ga on ga.name = m.teama
                            where fase = 'grupo B'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupb ga on ga.name = m.teamb
                            where fase = 'grupo B'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupc = PostgresOperator(
                task_id='update_table_groupc',
                postgres_conn_id='postgres-airflow',
                sql="""update groupc g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupc ga on ga.name = m.teama
                            where fase = 'grupo C'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupc ga on ga.name = m.teamb
                            where fase = 'grupo C'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupd = PostgresOperator(
                task_id='update_table_groupd',
                postgres_conn_id='postgres-airflow',
                sql="""update groupd g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupd ga on ga.name = m.teama
                            where fase = 'grupo D'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupd ga on ga.name = m.teamb
                            where fase = 'grupo D'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupe = PostgresOperator(
                task_id='update_table_groupe',
                postgres_conn_id='postgres-airflow',
                sql="""update groupe g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupe ga on ga.name = m.teama
                            where fase = 'grupo E'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupe ga on ga.name = m.teamb
                            where fase = 'grupo E'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupf = PostgresOperator(
                task_id='update_table_groupf',
                postgres_conn_id='postgres-airflow',
                sql="""update groupf g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupf ga on ga.name = m.teama
                            where fase = 'grupo F'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupf ga on ga.name = m.teamb
                            where fase = 'grupo F'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_groupg = PostgresOperator(
                task_id='update_table_groupg',
                postgres_conn_id='postgres-airflow',
                sql="""update groupg g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupg ga on ga.name = m.teama
                            where fase = 'grupo G'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join groupg ga on ga.name = m.teamb
                            where fase = 'grupo G'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )
        
        update_table_grouph = PostgresOperator(
                task_id='update_table_grouph',
                postgres_conn_id='postgres-airflow',
                sql="""update grouph g
                        set 
                        points = m.points, 
                        wins = m.wins, 
                        draws = m.draws, 
                        loses = m.loses,
                        ga = m.ga,
                        gc = m.gc
                        from (
                            select
                            teama as name,
                            sum(ga) as ga,
                            sum(gc) as gc,
                            sum(wins*3)+sum(draws) as points,
                            sum(wins) as wins,
                            sum(loses) as loses,
                            sum(draws) as draws
                            from(
                            select
                            m.teama,
                            goalsa as ga,
                            goalsb as gc,
                            case
                                when goalsa > goalsb then 1
                                else 0
                            end wins,
                            case
                                when goalsa < goalsb then 1
                                else 0
                            end loses,
                            case 
                                when goalsa = goalsb then 1
                                else 0
                            end draws
                            from matchs m
                            inner join grouph ga on ga.name = m.teama
                            where fase = 'grupo H'
                            union
                            select
                            m.teamb,
                            goalsb as ga,
                            goalsa as gc,
                            case
                                when goalsb > goalsa then 1
                                else 0
                            end wins,
                            case
                                when goalsb < goalsa then 1
                                else 0
                            end loses,
                            case 
                                when goalsb = goalsa then 1
                                else 0
                            end draws
                            from matchs m
                            inner join grouph ga on ga.name = m.teamb
                            where fase = 'grupo H'
                            ) matchs
                            group by matchs.teama
                            order by points desc, ga desc, gc asc
                        ) m
                        where g.name  = m.name"""
            )

        [update_table_groupa, update_table_groupd, update_table_groupc, update_table_groupd, update_table_groupe, update_table_groupf, update_table_groupg, update_table_grouph]
    
    with TaskGroup(group_id='create_table_classified_teams_A_H') as create_table_classified_teams_A_H:
        
        truncate_table_classified_teams = PostgresOperator(
            task_id='truncate_table_classified_teams',
            postgres_conn_id='postgres-airflow',
            sql="""
                truncate table classified_teams;
            """
        )
        
        create_table_classified_teams = PostgresOperator(
            task_id='create_table_classified_teams',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists classified_teams (
                teamid varchar(2),
                team varchar(50)
                )
            """
        )

        insert_classified_teams_groupa = PostgresOperator(
            task_id='insert_classified_teams_groupa',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('A',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupa
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupb = PostgresOperator(
            task_id='insert_classified_teams_groupb',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('B',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupb
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupc = PostgresOperator(
            task_id='insert_classified_teams_groupc',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('C',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupc
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupd = PostgresOperator(
            task_id='insert_classified_teams_groupd',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('D',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupd
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupe = PostgresOperator(
            task_id='insert_classified_teams_groupe',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('E',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupe
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupf = PostgresOperator(
            task_id='insert_classified_teams_groupf',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('F',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupf
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_groupg = PostgresOperator(
            task_id='insert_classified_teams_groupg',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('G',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from groupg
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )

        insert_classified_teams_grouph = PostgresOperator(
            task_id='insert_classified_teams_grouph',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                teamid,
                name
                from (
                    select 
                    concat('H',row_number () over(order by points desc, ga desc, gc asc)) teamid,
                    *
                    from grouph
                    order by points desc, ga desc, gc asc
                ) g
                limit 2
            """
        )
    
        truncate_table_classified_teams >> create_table_classified_teams >> [insert_classified_teams_groupa,insert_classified_teams_groupb,insert_classified_teams_groupc,insert_classified_teams_groupd,insert_classified_teams_groupe,insert_classified_teams_groupf,insert_classified_teams_groupg,insert_classified_teams_grouph]
    
    with TaskGroup(group_id='oitavas_matchs') as oitavas_matchs:

        drop_table_oitavas = PostgresOperator(
            task_id='drop_table_oitavas',
            postgres_conn_id='postgres-airflow',
            sql="""drop table if exists oitavas;"""
        )

        create_table_oitavas = PostgresOperator(
            task_id='create_table_oitavas',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists oitavas (
                teama varchar(50),
                teamb varchar(50),
                winner_teama boolean,
                winner_teamb boolean,
                matchs varchar(20)
                );
            """
        )

        insert_oitavas_matchs = PostgresOperator(
            task_id='insert_oitavas_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into matchs (teama, teamb, fase)
                select
                (select team from classified_teams where teamid = 'A1') teama,
                (select team from classified_teams where teamid = 'B2') teamb,
                'oitavas_A1B2_O1' fase
                union all
                select
                (select team from classified_teams where teamid = 'A2') teama,
                (select team from classified_teams where teamid = 'B1') teamb,
                'oitavas_A2B1_O2' fase
                union all
                select
                (select team from classified_teams where teamid = 'C1') teama,
                (select team from classified_teams where teamid = 'D2') teamb,
                'oitavas_C1D2_O3' fase
                union all
                select
                (select team from classified_teams where teamid = 'C2') teama,
                (select team from classified_teams where teamid = 'D1') teamb,
                'oitavas_C2D1_O4' fase
                union all
                select
                (select team from classified_teams where teamid = 'E1') teama,
                (select team from classified_teams where teamid = 'F2') teamb,
                'oitavas_E1F2_O5' fase
                union all
                select
                (select team from classified_teams where teamid = 'E2') teama,
                (select team from classified_teams where teamid = 'F1') teamb,
                'oitavas_E2F1_O6' fase
                union all
                select
                (select team from classified_teams where teamid = 'G1') teama,
                (select team from classified_teams where teamid = 'H2') teamb,
                'oitavas_G1H2_O7' fase
                union all
                select
                (select team from classified_teams where teamid = 'G2') teama,
                (select team from classified_teams where teamid = 'H1') teamb,
                'oitavas_G2H1_O8' fase
            """
        )

        updates_oitavas_matchs = PostgresOperator(
            task_id='updates_oitavas_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                update matchs
                set
                goalsa = floor(((random() + random())* 4))::int,
                goalsb = floor(((random() + random())* 4))::int
                where fase like '%oitavas%';

                update matchs
                set
                draw = (case when goalsa = goalsb
                            then True
                            else False
                    end)
                where fase like '%oitavas%';

                update matchs
                set
                penaltis = (case when draw = True
                                then True
                                else False
                            end)
                where fase like '%oitavas%';

                update matchs
                set
                penaltisa = floor((random() * 4+1))::int,
                penaltisb = floor((random() * 4+1))::int
                where fase like '%oitavas%' and penaltis = true;
            """
        )

        insert_table_oitavas = PostgresOperator(
            task_id='insert_table_oitavas',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into oitavas
                select
                teama,
                teamb,
                case
                    when goalsa > goalsb then true
                    when draw = true and penaltisa > penaltisb then true
                    when draw = true and penaltisa = penaltisb then true
                    else false
                end winner_teama,
                case
                    when goalsa < goalsb then true
                    when draw = true and penaltisa < penaltisb then true
                    else false
                end winner_teamb,
                substring(fase,9,7) as matchs 
                from matchs
                where fase like '%oitavas%'
            """
        )
    
        drop_table_oitavas >> create_table_oitavas >> insert_oitavas_matchs >> updates_oitavas_matchs >> insert_table_oitavas

    with TaskGroup(group_id='quartas_matchs') as quartas_matchs:

        insert_classified_teams_quartas = PostgresOperator(
            task_id='insert_classified_teams_quartas',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                substring(matchs,6,2) as teamid,
                case 
                    when winner_teama = true then teama
                    else teamb
                end as name
                from oitavas;
                """
        )

        drop_table_quartas = PostgresOperator(
            task_id='drop_table_quartas',
            postgres_conn_id='postgres-airflow',
            sql="""drop table if exists quartas;"""
        )

        create_table_quartas = PostgresOperator(
            task_id='create_table_quartas',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists quartas (
                teama varchar(50),
                teamb varchar(50),
                winner_teama boolean,
                winner_teamb boolean,
                matchs varchar(20)
                );
            """
        )

        insert_quartas_matchs = PostgresOperator(
            task_id='insert_quartas_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into matchs (teama, teamb, fase)
                select
                (select team from classified_teams where teamid = 'O1') teama,
                (select team from classified_teams where teamid = 'O3') teamb,
                'quartas_O1O3_S1' fase
                union all
                select
                (select team from classified_teams where teamid = 'O2') teama,
                (select team from classified_teams where teamid = 'O4') teamb,
                'quartas_O2O4_S2' fase
                union all
                select
                (select team from classified_teams where teamid = 'O5') teama,
                (select team from classified_teams where teamid = 'O7') teamb,
                'quartas_O5O7_S3' fase
                union all
                select
                (select team from classified_teams where teamid = 'O6') teama,
                (select team from classified_teams where teamid = 'O8') teamb,
                'quartas_O6O8_S4' fase
            """
        )

        updates_quartas_matchs = PostgresOperator(
            task_id='updates_quartas_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                update matchs
                set
                goalsa = floor(((random() + random())* 4))::int,
                goalsb = floor(((random() + random())* 4))::int
                where fase like '%quartas%';

                update matchs
                set
                draw = (case when goalsa = goalsb
                            then True
                            else False
                    end)
                where fase like '%quartas%';

                update matchs
                set
                penaltis = (case when draw = True
                                then True
                                else False
                            end)
                where fase like '%quartas%';

                update matchs
                set
                penaltisa = floor((random() * 4+1))::int,
                penaltisb = floor((random() * 4+1))::int
                where fase like '%quartas%' and penaltis = true;
            """
        )

        insert_table_quartas = PostgresOperator(
            task_id='insert_table_quartas',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into quartas
            select
            teama,
            teamb,
            case
                when goalsa > goalsb then true
                when draw = true and penaltisa > penaltisb then true
                when draw = true and penaltisa = penaltisb then true
                else false
            end winner_teama,
            case
                when goalsa < goalsb then true
                when draw = true and penaltisa < penaltisb then true
                else false
            end winner_teamb,
            substring(fase,9,7) as matchs 
            from matchs
            where fase like '%quartas%'
            """
        )

        insert_classified_teams_quartas >> drop_table_quartas >> create_table_quartas >> insert_quartas_matchs >> updates_quartas_matchs >> insert_table_quartas
    
    with TaskGroup(group_id='semis_matchs') as semis_matchs:

        insert_classified_teams_semis = PostgresOperator(
            task_id='insert_classified_teams_semis',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                substring(matchs,6,2) as teamid,
                case 
                    when winner_teama = true then teama
                    else teamb
                end as name
                from quartas;
                """
        )

        drop_table_semis = PostgresOperator(
            task_id='drop_table_semis',
            postgres_conn_id='postgres-airflow',
            sql="""drop table if exists semis;"""
        )

        create_table_semis = PostgresOperator(
            task_id='create_table_semis',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists semis (
                teama varchar(50),
                teamb varchar(50),
                winner_teama boolean,
                winner_teamb boolean,
                matchs varchar(20)
                );
            """
        )

        insert_semis_matchs = PostgresOperator(
            task_id='insert_semis_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into matchs (teama, teamb, fase)
                select
                (select team from classified_teams where teamid = 'S1') teama,
                (select team from classified_teams where teamid = 'S3') teamb,
                'semis_S1S3_X1' fase
                union all
                select
                (select team from classified_teams where teamid = 'S2') teama,
                (select team from classified_teams where teamid = 'S4') teamb,
                'semis_S2S4_X2' fase
            """
        )

        updates_semis_matchs = PostgresOperator(
            task_id='updates_semis_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                update matchs
                set
                goalsa = floor(((random() + random())* 4))::int,
                goalsb = floor(((random() + random())* 4))::int
                where fase like '%semis%';

                update matchs
                set
                draw = (case when goalsa = goalsb
                            then True
                            else False
                    end)
                where fase like '%semis%';

                update matchs
                set
                penaltis = (case when draw = True
                                then True
                                else False
                            end)
                where fase like '%semis%';

                update matchs
                set
                penaltisa = floor((random() * 4+1))::int,
                penaltisb = floor((random() * 4+1))::int
                where fase like '%semis%' and penaltis = true;
            """
        )

        insert_table_semis = PostgresOperator(
            task_id='insert_table_semis',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into semis
            select
            teama,
            teamb,
            case
                when goalsa > goalsb then true
                when draw = true and penaltisa > penaltisb then true
                when draw = true and penaltisa = penaltisb then true
                else false
            end winner_teama,
            case
                when goalsa < goalsb then true
                when draw = true and penaltisa < penaltisb then true
                else false
            end winner_teamb,
            substring(fase,7,8) as matchs 
            from matchs
            where fase like '%semis%'
            """
        )

        insert_classified_teams_semis >> drop_table_semis >> create_table_semis >> insert_semis_matchs >> updates_semis_matchs >> insert_table_semis

    with TaskGroup(group_id='final_matchs') as final_matchs:

        insert_classified_teams_final = PostgresOperator(
            task_id='insert_classified_teams_final',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into classified_teams
                select
                substring(matchs,6,2) as teamid,
                case 
                    when winner_teama = true then teama
                    else teamb
                end as name
                from semis;
                """
        )

        drop_table_final = PostgresOperator(
            task_id='drop_table_final',
            postgres_conn_id='postgres-airflow',
            sql="""drop table if exists final;"""
        )

        create_table_final = PostgresOperator(
            task_id='create_table_final',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists final (
                teama varchar(50),
                teamb varchar(50),
                winner_teama boolean,
                winner_teamb boolean,
                matchs varchar(20)
                );
            """
        )

        insert_final_matchs = PostgresOperator(
            task_id='insert_final_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into matchs (teama, teamb, fase)
                select
                (select team from classified_teams where teamid = 'X1') teama,
                (select team from classified_teams where teamid = 'X2') teamb,
                'final' fase
            """
        )

        updates_final_matchs = PostgresOperator(
            task_id='updates_final_matchs',
            postgres_conn_id='postgres-airflow',
            sql="""
                update matchs
                set
                goalsa = floor(((random() + random())* 4))::int,
                goalsb = floor(((random() + random())* 4))::int
                where fase like '%final%';

                update matchs
                set
                draw = (case when goalsa = goalsb
                            then True
                            else False
                    end)
                where fase like '%final%';

                update matchs
                set
                penaltis = (case when draw = True
                                then True
                                else False
                            end)
                where fase like '%final%';

                update matchs
                set
                penaltisa = floor((random() * 4+1))::int,
                penaltisb = floor((random() * 4+1))::int
                where fase like '%final%' and penaltis = true;
            """
        )

        insert_table_final = PostgresOperator(
            task_id='insert_table_final',
            postgres_conn_id='postgres-airflow',
            sql="""
            insert into final
            select
            teama,
            teamb,
            case
                when goalsa > goalsb then true
                when draw = true and penaltisa > penaltisb then true
                when draw = true and penaltisa = penaltisb then true
                else false
            end winner_teama,
            case
                when goalsa < goalsb then true
                when draw = true and penaltisa < penaltisb then true
                else false
            end winner_teamb,
            'final' as matchs 
            from matchs
            where fase like '%final%'
            """
        )

        insert_table_world_cup = PostgresOperator(
            task_id='insert_table_world_cup',
            postgres_conn_id='postgres-airflow',
            sql="""
                insert into world_cup
                select
                case
                    when winner_teama = True then teama
                    else teamb
                end as winner,
                case
                    when winner_teama = False then teama
                    else teamb
                end as runner,
                CURRENT_TIMESTAMP date
                from final
            """
        )

        insert_table_matchs_history = PostgresOperator(
            task_id='insert_table_matchs_history',
            postgres_conn_id='postgres-airflow',
            sql="""
                create table if not exists matchs_history as 
                select * from matchs;

                insert into matchs_history
                select * from matchs;
            """
        )

        insert_classified_teams_final >> drop_table_final >> create_table_final >> insert_final_matchs >> updates_final_matchs >> insert_table_final >> insert_table_world_cup >> insert_table_matchs_history
    
    end = EmptyOperator(task_id='end')

    start >> create_table_world_cup >> cup_table >> aux_table_teams >> create_groups_A_H >> create_functions_A_H >> call_functions_A_H >> create_matchs_groups_A_H >> update_table_groups_A_H >> \
    create_table_classified_teams_A_H >> oitavas_matchs >> quartas_matchs >> semis_matchs >> final_matchs >> end


    