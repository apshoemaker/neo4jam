package com.ideacandi.jams;

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

import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.Random;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

public class BarabasiAlbert implements IJam {
    private static enum RelTypes implements RelationshipType {
        CONNECTED
    }

    private GraphDatabaseService _graphdb;

    public static final Logger logger = Logger.getLogger("BarabasiAlbert");

    public BarabasiAlbert(){}

    @Override
    public void godo(GraphDatabaseService graphdb){
        this._graphdb = graphdb;
        logger.debug("Generating BA model ...");
        this._generateBarabasiAlbert(1000, 3);
    }

    /**
     * mainly a transaction wrapper - the fun happens below
     */
    private void _generateBarabasiAlbert(int n, int m){
        /*
         * first node should be labeled root, so we know where
         * we started
         */
        boolean firstNode = true;
        Transaction tx = null;
        try{
            int batch = 10000;
            tx = this._graphdb.beginTx();

            //if graph is empty - need seeded graph
            //@reference http://en.wikipedia.org/wiki/Barab%C3%A1si%E2%80%93Albert_model Algorithm section
            if(this._isEmptyGraph()){
                this._startGeneration();
            }

            int i = 0; //counter for batching - you'll thank me later
            while(i <= n){
                logger.info("Creating node " + i);
                if(++i % batch == 0){
                    tx.success();
                    tx.close();
                    tx = this._graphdb.beginTx();
                }

                //attach edges from node - number of edges determined randomly between 1 and m
                int numberOfEdges = this._getRandomNumberOfEdgesFor(m);
                logger.debug("Number of Edges: " + numberOfEdges);

                //attach node n to m edges
                logger.debug("Attaching node to random node(s) according to random edge value ...");
                this._attachNodeWithEdges(numberOfEdges);
            }

            tx.success();
            tx.close();
        } catch (Exception ex){
            logger.debug("ERR");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.debug(sw.toString());
            tx.close();
        }
    }

    /**
     * choose a random node i - random between 0 and number of nodes
     * find degree of chosen node i
     * calc probability of attachment where probability = degree of node / nodes.size() * 2
     * if attach probability >= next random double - attach else choose new node
     * 
     * @reference http://en.wikipedia.org/wiki/Barab%C3%A1si%E2%80%93Albert_model
     */
    private void _attachNodeWithEdges(int numberOfEdges){
        Random random = new Random();
        /*
         * we want to make sure we don't use the possibly newly attached node edge
         */
        int sumDegree = this._sumDegreeOfGraph();

        /*
         * create node
         */
        String nodeName = String.format("child");
        Node node = this._createNode(nodeName);
        for(int i = 0; i < numberOfEdges; i++){
            boolean createEdge = false;

            Node randomNode = null;
            do{
                //choose random node
                randomNode = this._chooseRandomNode(random);
                logger.debug("Random node chosen ..");

                int nodeDegree = randomNode.getDegree();
                logger.debug("Random node has nodeDegree " + nodeDegree);

                //calc probability of attachment
                logger.debug(nodeDegree + " / " + sumDegree);
                double attachProbability = ((nodeDegree + 0.0) / (sumDegree + 0.0));
                logger.debug("Random node has attachProbability of " + attachProbability);

                //if can attach, attach - otherwise we are trying again
                double nextRandomDouble = random.nextDouble();
                logger.debug("Next random double:  " + nextRandomDouble);
                if(attachProbability >= nextRandomDouble){
                    createEdge = true;
                    logger.debug("Attaching node to chosen random node ...");
                }
            } while(!createEdge);


            //create edge relationship between nodes
            node.createRelationshipTo(randomNode, RelTypes.CONNECTED);
            logger.debug("Relationship connected from node to random node ...");
        }
    }

    /**
     * convenience method to choose a random node
     */
    private Node _chooseRandomNode(Random random){
        GlobalGraphOperations graphops = GlobalGraphOperations.at(this._graphdb);
        Iterable<Node> nodes = graphops.getAllNodes();
        List<Node> nodesList = IteratorUtil.asList(nodes);

        return nodesList.get(random.nextInt(nodesList.size()));
    }

    /**
     * get the degree sum of the graph
     */
    private int _sumDegreeOfGraph(){
        GlobalGraphOperations graphops = GlobalGraphOperations.at(this._graphdb);
        Iterable<Node> nodes = graphops.getAllNodes();

        return IteratorUtil.count(nodes) * 2;
    }

    /**
     * generate a random number between 1 and m
     * @reference http://stackoverflow.com/questions/363681/generating-random-integers-in-a-range-with-java
     * @param m - max value
     * @return int
     */
    private int _getRandomNumberOfEdgesFor(int m){
        Random random = new Random();
        return random.nextInt((m - 1) + 1) + 1;
    }


    /**
     * convenience method to create our new graph
     * since we must not have an existing graph to play with
     */
    private void _startGeneration(){
        Node rootNode = this._createNode("root");
        Node childNode = this._createNode("child");
        rootNode.createRelationshipTo(childNode, RelTypes.CONNECTED);
    }

    /**
     * convenience method to create a node
     */
    private Node _createNode(String lbl){
        Node node = this._graphdb.createNode();
        Label label = DynamicLabel.label(lbl);
        node.addLabel(label);

        return node;
    }

    /**
     * check if our graph is empty
     * empty means 0 nodes
     */
    private boolean _isEmptyGraph(){
        GlobalGraphOperations graphops = GlobalGraphOperations.at(this._graphdb);
        Iterable<Node> nodes = graphops.getAllNodes();
        if(IteratorUtil.count(nodes) > 0){
            logger.debug("Existing graph ...");
            return false;
        }

        logger.debug("New graph ...");
        return true;
    }
}
