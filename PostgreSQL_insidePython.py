-- These are PostgreSQL queries to be used inside Python and Pandas SQL Connector.


table_name_untrained = 'public."BeatData"'

sampleCount_threshold = 50000

table_name_trained = 'public."BeatClassifierKNN"'

untrained_sessions_query = """ Select untrained.sessionId,
							 untrained.timestamp, untrained.type, untrained.heart_rate, untrained.qrs_interval, untrained.pr_interval, untrained.cluster_group
						 From ( Select distinct \"sessionId\"
								From %s
								Where type IN (4,5,6)
								Group By \"sessionId\"
								Having Count(id) > %d ) untrained left join %s trained ON untrained.\"sessionId\"=trained.\"sessionId\"
						 Where trained.\"sessionId\" IS NULL  """ % (table_name_untrained, sampleCount_threshold, table_name_trained)

untrained_sessions_df = pd.read_sql(untrained_sessions_query, conn)

#--------------------------------------------------------------------------------

missing_cg_query = """  Select untr.\"sessionId\", untr.timestamp, untr.type, untr.heart_rate, untr.qrs_interval, untr.pr_interval, untr.cluster_group
						From (	Select distinct \"sessionId\"
								From %s
								Where type IN (4,5,6) ) tr JOIN (Select \"sessionId\", timestamp, type, heart_rate, 
																 qrs_interval, pr_interval, cluster_group 
																 From %s 
																 Where cluster_group IS NULL and type IN (4,5,6) ) untr ON tr.\"sessionId\"=untr.\"sessionId\" """ % (table_name_trained, table_name_untrained)

missing_cg_df = pd.read_sql(missing_cg_query, conn)

Select untr."sessionId", untr.timestamp, untr.type, untr.heart_rate, untr.qrs_interval, untr.pr_interval, untr.cluster_group
                    From ( Select Distinct "sessionId"
                    	   From public."BeatClassifierKNN" Where type IN (4,5,6) ) tr 
                    
                    JOIN ( Select "sessionId", untr.timestamp, untr.type, untr.heart_rate, untr.qrs_interval, untr.pr_interval, untr.cluster_group 
                    	   From public."BeatData" Where cluster_group IS NULL and type IN (4,5,6) ) untr ON tr."sessionId"=untr."sessionId"

#--------------------------------------------------------
