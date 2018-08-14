package com.sephora.avro;

import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Converts xml into internal representation based on TreeNode class
 */
public class XmlParser {

    public XmlParser() {
    }

    /**
     * Parses given xml file into TreeNode representation
     * @param file input xml file to parse
     * @return root node of the TreeNode representation
     */
    public static TreeNode parse(InputStream file) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(file);

        TreeNode result = parseNode(new TreeNode(), doc.getDocumentElement());

        return result;
    }

    /**
     * Checks that current xml node contains nothing except pure text
     * Examples:
     *  - &lt;a&gt;42&lt;/a&gt;
     *  - &lt;a&gt;&lt;![CDATA[42]]&gt;&lt;\a&gt;
     * @param node xml node to check
     * @return true if node contains only pure text
     */
    private static boolean isPureTextNode(Node node) {
        // almost every node has child text node but text contains formatting from
        // xml, several white spaces and eol characters.
        // CDATA is a bit different: it has explicit tag and
        // whitespaces and empty strings are significant
        if (node.hasChildNodes()) {
            NodeList nodes = node.getChildNodes();
            if (nodes.getLength() == 1) {
                Node one = nodes.item(0);
                short nodeType = one.getNodeType();
                String value = nodes.item(0).getNodeValue();
                if (nodeType == Node.CDATA_SECTION_NODE ||
                        (nodeType == Node.TEXT_NODE && !value.trim().equals(""))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Shorthand to extract pure text from the xml node
     * @param node xml node to extract text
     * @return text that was extracted from the xml node
     */
    private static String getText(Node node) {
        return node.getChildNodes().item(0).getNodeValue();
    }

    /**
     * Removes inapropriate content from xml names
     * @param name string to cleanup
     * @return cleaned up string
     */
    private static String filterName(String name) {
        // avro names can't contain colon symbol.
        // Actually avro names can't contain anything besides [A-Za-z0-9_]
        if (name.contains(":")) {
            String[] parts = name.split(":");
            return parts[parts.length - 1];
        }
        return name;
    }

    /**
     * Converts xml node to TreeNode and add to parent
     * @param parent parent node for TreeNode representation
     * @param xmlNode parent for xml representation
     * @return TreeNode representation of the current xml node
     */
    private static TreeNode parseNode(TreeNode parent, Node xmlNode) {
        TreeNode treeNode = new TreeNode();

        if (isPureTextNode(xmlNode)) {
            // ideally we shouldn't have else if we have a text node, but xml
            // still allows to add attributes. It's impossible to have both
            // in avro, but we can't decide what to choose here, we need schema
            // so such ambiguity will be resolved in the encoding step
            treeNode.setData(getText(xmlNode));
        }
        parent.addField(filterName(xmlNode.getNodeName()), treeNode);

        // subnodes require complex processing, calling recursively
        if (xmlNode.hasChildNodes()) {
            NodeList xmlChildren = xmlNode.getChildNodes();
            for (int i = 0; i < xmlChildren.getLength(); i++) {
                Node xmlChild = xmlChildren.item(i);
                if (xmlChild.getNodeType() == Node.ELEMENT_NODE) {
                    parseNode(treeNode, xmlChild);
                }
            }
        }

        // attributes are converted into simple fields for current node
        if (xmlNode.hasAttributes()) {
            NamedNodeMap attrs = xmlNode.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                Node attr = attrs.item(i);
                TreeNode attrNode = new TreeNode(attr.getNodeValue());
                String name = filterName(attr.getNodeName());
                treeNode.addField(name, name + "_attr", attrNode);
            }
        }

        return parent;
    }
}
