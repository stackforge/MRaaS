package com.hpcloud.mraas.ssh;
import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.ConnectionInfo;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import com.yammer.dropwizard.logging.Log;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class SSH {
    private static final Log LOG = Log.forClass(SSH.class);

    public static void ssh_cmd(String host, String privateKey, String command) {
        LOG.debug("running {} on {}", command, host);

        try {
            Connection conn = getAuthedConnection(host, privateKey);
            Session session = conn.openSession();
            session.execCommand(command);

            session.waitForCondition(ChannelCondition.EXIT_STATUS, 1000000);
/*
            System.out.println(gobbleStream(session.getStdout());
*/
            System.out.println( "ExitCode: " + session.getExitStatus() );

            // Close the session
            session.close();

        } catch (Exception e) {
            System.out.println(e); //TODO
            e.printStackTrace(System.out);
        }
    }


    private static String gobbleStream(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        InputStream read = new StreamGobbler(in);
        BufferedReader br = new BufferedReader(new InputStreamReader(read));
        String line = br.readLine();
        while( line != null ) {
            sb.append( line + "\n" );
            line = br.readLine();
        }
        return sb.toString();
    }

    private static Connection getAuthedConnection(String host, String privateKey) throws Exception {
        Connection conn = new Connection(host);
        ConnectionInfo info = conn.connect();
        conn.authenticateWithPublicKey("root", privateKey.toCharArray(), "");
        return conn;
    }

    public static String getRemoteFile(String host, String privateKey, String remoteFile) {
        LOG.debug("getting file {} from {}", remoteFile, host);
        try {
            Connection conn = getAuthedConnection(host, privateKey);
            SCPClient scp = new SCPClient(conn);
            OutputStream out = new ByteArrayOutputStream();
            scp.get(remoteFile, out);
            InputStream in = new ByteArrayInputStream(((ByteArrayOutputStream) out).toByteArray());
            return gobbleStream(in);
        } catch (Exception e) {
            //TODO
            System.out.println(e);
            e.printStackTrace(System.out);
        }
        return null;
    }

}
