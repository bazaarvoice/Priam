package com.netflix.priam.identity;

import java.util.List;
import java.util.Map;

/**
 * Interface for managing Cassandra instance data. Provides functionality
 * to register, update, delete or list instances from the registry
 */
public interface IPriamInstanceRegistry {
    /**
     * Return a list of all Cassandra server nodes registered.
     *
     * @param appName the cluster name
     * @return a list of all nodes in {@code appName}
     */
    List<PriamInstance> getAllIds(String appName);

    /**
     * Return the Cassandra server node with the given {@code id}.
     *
     * @param appName the cluster name
     * @param id      the node id
     * @return the node with the given {@code id}, or {@code null} if none found
     */
    PriamInstance getInstance(String appName, int id);

    /**
     * Create/Register an instance of the server with its info.
     *
     * @return the new node
     */
    PriamInstance create(String app, int id, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token);

    /**
     * Delete the server node from the registry
     *
     * @param inst the node to delete
     */
    void delete(PriamInstance inst);

    /**
     * Update the details of the server node in registry
     *
     * @param inst the node to update
     */
    void update(PriamInstance inst);

    /**
     * Overwrite the details of the server node in registry if it matches the provided instance ID
     *
     * @param inst the node to update
     */
    boolean acquireInstanceId(int slotId, PriamInstance inst, String expectedInstanceId);

}