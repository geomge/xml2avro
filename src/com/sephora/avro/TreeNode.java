package com.sephora.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Intermediate representation of the xml to handle structures like arrays.
 */
public class TreeNode {
    /**
     * Named objects from the xml node, may be child nodes or attributes.
     *
     * Tags and attributes are treated equally. Objects with the same name
     * will be just appended to the list.
     * Tags and attributes with the same name will be treated as an array!
     */
    private Map<String, List<TreeNode>> fields;

    /**
     * Text value of the xml node.
     * Currently it may be a pure text value like <a>42</a> == 42, or
     * CDATA encoded value like <a><![CDATA[42]]><\a> == 42
     */
    private String nodeValue;

    public TreeNode() {
    }

    /**
     * Shorthand to create a node with text value stored
     * @param value text value to store
     */
    public TreeNode(String value) {
        nodeValue = value;
    }

    /**
     * Adds a named value to current node
     * @param name name of the field
     * @param value new node that represents the field
     */
    public void addField(String name, TreeNode value) {
        if (fields == null) {
            fields = new HashMap<String, List<TreeNode>>();
        }

        fields.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }

    /**
     * Adds a named value to current node
     * @param name name of the field
     * @param alterName alternative name of the field if first is already exists
     * @param value new node that represents the field
     */
    public void addField(String name, String alterName, TreeNode value) {
        if (fields == null) {
            fields = new HashMap<String, List<TreeNode>>();
        }

        if (fields.containsKey(name)) {
            fields.computeIfAbsent(alterName, k -> new ArrayList<>()).add(value);
        } else {
            fields.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
        }
    }

    /**
     * @return a set of field names for current node
     */
    public Set<String> getKeys() {
        if (fields == null) {
            // empty set is needed for type validation on empty unions
            return new HashSet<>();
        }
        return fields.keySet();
    }

    /**
     * Retrieves all available nodes for given field name
     * @param fieldName field name to to get values
     * @return list of nodes for given field name
     */
    public List<TreeNode> getValues(String fieldName) {
        // specific node can be empty, but we still need to check for default
        // fields that present in the schema
        if (fields == null) {
            return new ArrayList<TreeNode>();
        }
        return fields.get(fieldName);
    }

    /**
     * Similar to {@link #getValues(String)} but checks that fields has only one node
     * @param fieldName field name to get value
     * @return node for given field name
     * @throws IllegalArgumentException if {@link #fields} has 2+ items for fieldName
     */
    public TreeNode getValue(String fieldName) {
        List<TreeNode> fieldsByName = getValues(fieldName);
        return ensureSingle(fieldsByName, fieldName);
    }

    public static TreeNode ensureSingle(List<TreeNode> fieldsByName, String fieldName) {
        if (fieldsByName.size() != 1) {
            // schema violation: several tags for one field name
            // xml contains an array, but schema has a record
            // can't handle such ambiguity during conversion
            String msg = String.format("Xml structure issue: %d values for " +
                    "field name %s. Possibly array is needed in schema.", fieldsByName.size(), fieldName);
            throw new IllegalArgumentException(msg);
        }
        return fieldsByName.get(0);
    }

    public String getData() {
        return nodeValue;
    }

    public void setData(String value) {
        nodeValue = value;
    }

    @Override
    public String toString() {
        return toString("| ");
    }

    /**
     * Formats node and its children as indented "key : value" text
     * @return string representation of the nodes tree
     */
    String toString(String prefix) {
        String value = "";
        if (fields != null) {
            for (Map.Entry<String, List<TreeNode>> entry: fields.entrySet()) {
                for (TreeNode node: entry.getValue()) {
                    String nodeToString = node.toString(prefix + "  ");
                    value += String.format("%s%s : %s\n%s", prefix, entry.getKey(),
                            node.nodeValue, nodeToString);
                }
            }
        }
        return value;
    }
}
