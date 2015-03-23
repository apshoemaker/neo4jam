package com.ideacandi;

import org.apache.log4j.Logger;
import com.ideacandi.jams.BarabasiAlbert;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.helpers.collection.IteratorUtil;

public class Neo4Jam {
    public static final Logger logger = Logger.getLogger("Neo4Jams");

    private GraphDatabaseService _graphdb;

    /**
     * let's get this party started
     */
    public static void main(final String[] args){
        if(args.length != 2){
            logger.error("Usage:  neo4jam <algo> <graph_file>");
        } else {
            String algo = args[0];
            String graphFile = args[1];

            switch(algo){
                case "ba":
                    logger.info("Barabasi Albert warming up ...");
                    GraphDatabaseService graphdb = embedGraphDb(graphFile);
                    BarabasiAlbert ba = new BarabasiAlbert();
                    ba.godo(graphdb);
            }
        }
    }
    
    /**
     * create a new database or embed from an existing path
     */
    public static GraphDatabaseService embedGraphDb(String graphFile){
        logger.debug("Creating database ...");

        GraphDatabaseService graphdb = new GraphDatabaseFactory().newEmbeddedDatabase(graphFile);
        registerShutdownHook(graphdb);

        return graphdb;
    }

    /**
     * async hook to shutdown the db file cleanly
     */
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
                {
                    @Override
                    public void run()
                    {
                        graphDb.shutdown();
                    }
                } );
    }
}
