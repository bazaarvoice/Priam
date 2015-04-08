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
     * Updates the value of an existing registry entry, provided that the current value matches what the caller expects
     * (this acts as a basic test-and-set mechanism for updates)
     *
     * @return the new node, or null if it failed to acquire the slot
     */
    PriamInstance acquireSlotId(int slotId, String expectedInstanceId, String app, String instanceID, String hostname, String ip, String rac, Map<String, Object> volumes, String token);

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

}