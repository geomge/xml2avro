package com.sephora.avro;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

    public static void main (String[] args){
        // convertXml2Avro("test","test");
        //This class is used for testing, so it has hardcoded values
        try{
            String xml=readFile("C:\\Users\\Litmus7\\Desktop\\avro\\QA_POSLOG.xml", Charset.defaultCharset());
            String avsc=readFile("C:\\Users\\Litmus7\\Desktop\\avro\\Sephora_POS.avsc", Charset.defaultCharset());

            //XmlParser parser = new XmlParser();
            TreeNode root = XmlParser.parse(new ByteArrayInputStream(xml.getBytes()));

            ByteArrayOutputStream avro = new ByteArrayOutputStream();
            AvroWriter avroWriter = new AvroWriter(new ByteArrayInputStream(avsc.getBytes()));
            avroWriter.write(root, avro);

            writeFile("C:\\Users\\Litmus7\\Desktop\\avro\\message.avro", avro);

            System.out.println(new String(avro.toByteArray()));

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    //helper methods to read and write loacl files

    private static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static void writeFile(String path, ByteArrayOutputStream baos)
            throws IOException
    {
        try(OutputStream outputStream= new FileOutputStream(path)) {
            baos.writeTo(outputStream);
        }
    }
}
