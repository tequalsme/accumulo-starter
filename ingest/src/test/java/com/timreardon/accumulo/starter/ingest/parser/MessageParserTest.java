package com.timreardon.accumulo.starter.ingest.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.parser.MessageParser;

public class MessageParserTest {
    private static final String DATA_FILE_1 = "samples/maildir/allen-p/_sent_mail/1.";
    private static final String DATA_FILE_2 = "samples/maildir/mcconnell-m/all_documents/520.";
    private static final String DATA_FILE_3 = "samples/maildir/skilling-j/inbox/1.";
    private static final String DATA_FILE_4 = "samples/maildir/skilling-j/inbox/genie/1.";
    private static final String DATA_FILE_5 = "samples/maildir/skilling-j/sent/83.";

    private MessageParser parser = new MessageParser();
    
    @Test
    public void test1() throws Exception {
        String path = DATA_FILE_1;
        Message m = parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
        assertNotNull(m.getId());
        assertTrue(m.getTimestamp() > 0);
        
        assertEquals("allen-p", m.getMailbox());
        assertEquals("_sent_mail", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("phillip.allen@enron.com", m.getFrom());
        assertEquals("tim.belden@enron.com", m.getToAsString());
        assertNull(m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
//        assertEquals("1.0", m.getHeaders().get("Mime-Version").iterator().next());
//        assertEquals("text/plain; charset=us-ascii", m.getHeaders().get("Content-Type").iterator().next());
//        assertEquals("7bit", m.getHeaders().get("Content-Transfer-Encoding").iterator().next());
//        assertEquals("Sherri Sera", m.getHeaders().get("X-From").iterator().next());
//        assertEquals("\"Stephen L Comeau\" <slc1856@sunflower.com>", m.getHeaders().get("X-To").iterator().next());
//        assertEquals("\\Jeffrey_Skilling_Dec2000\\Notes Folders\\Sent", m.getHeaders().get("X-Folder").iterator().next());
//        assertEquals("SKILLING-J", m.getHeaders().get("X-Origin").iterator().next());
//        assertEquals("jskillin.nsf", m.getHeaders().get("X-FileName").iterator().next());
        
//        assertEquals(11, m.getHeaders().size());
        assertEquals(4, m.getBodyTokens().size());
        assertEquals(508, m.getRawBytes().length);
    }
    
    @Test
    public void test2() throws Exception {
        String path = DATA_FILE_2;
        Message m = parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
        assertNotNull(m.getId());
        assertTrue(m.getTimestamp() > 0);
        
        assertEquals("mcconnell-m", m.getMailbox());
        assertEquals("all_documents", m.getFolder());
        assertEquals("520.", m.getFileName());
        
        assertEquals("enron.announcements@enron.com", m.getFrom());
        assertEquals("ena.employees@enron.com", m.getToAsString());
        assertEquals("Re-Alignment", m.getSubject());
        assertEquals("joe.kishkill@enron.com,orlando.gonzalez@enron.com,brett.wiggs@enron.com,remi.collonges@enron.com,jeffrey.shankman@enron.com,mike.mcconnell@enron.com,jeffrey.mcmahon@enron.com,raymond.bowen@enron.com,louise.kitchen@enron.com,philippe.bibi@enron.com,rebecca.mcdonald@enron.com,james.hughes@enron.com,mark.frevert@enron.com,greg.whalley@enron.com,richard.shapiro@enron.com,steven.kean@enron.com,james.steffes@enron.com,ben.glisan@enron.com,mark.koenig@enron.com,rick.buy@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,kenneth.lay@enron.com,cliff.baxter@enron.com,michael.brown@enron.com,mark.palmer@enron.com",m.getCcAsString());
        assertEquals("joe.kishkill@enron.com,orlando.gonzalez@enron.com,brett.wiggs@enron.com,remi.collonges@enron.com,jeffrey.shankman@enron.com,mike.mcconnell@enron.com,jeffrey.mcmahon@enron.com,raymond.bowen@enron.com,louise.kitchen@enron.com,philippe.bibi@enron.com,rebecca.mcdonald@enron.com,james.hughes@enron.com,mark.frevert@enron.com,greg.whalley@enron.com,richard.shapiro@enron.com,steven.kean@enron.com,james.steffes@enron.com,ben.glisan@enron.com,mark.koenig@enron.com,rick.buy@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,kenneth.lay@enron.com,cliff.baxter@enron.com,michael.brown@enron.com,mark.palmer@enron.com",m.getBccAsString());
//        assertEquals("1.0", m.getHeaders().get("Mime-Version").iterator().next());
//        assertEquals("text/plain; charset=us-ascii", m.getHeaders().get("Content-Type").iterator().next());
//        assertEquals("7bit", m.getHeaders().get("Content-Transfer-Encoding").iterator().next());
//        assertEquals("Sherri Sera", m.getHeaders().get("X-From").iterator().next());
//        assertEquals("\"Stephen L Comeau\" <slc1856@sunflower.com>", m.getHeaders().get("X-To").iterator().next());
//        assertEquals("\\Jeffrey_Skilling_Dec2000\\Notes Folders\\Sent", m.getHeaders().get("X-Folder").iterator().next());
//        assertEquals("SKILLING-J", m.getHeaders().get("X-Origin").iterator().next());
//        assertEquals("jskillin.nsf", m.getHeaders().get("X-FileName").iterator().next());
        
//        assertEquals(11, m.getHeaders().size());
        assertEquals(630, m.getBodyTokens().size());
        assertEquals(14907, m.getRawBytes().length);
    }
    
    @Test
    public void test3() throws Exception {
        String path = DATA_FILE_3;
        Message m = parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
        assertNotNull(m.getId());
        assertTrue(m.getTimestamp() > 0);
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("inbox", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("dorsey@enron.com", m.getFrom());
        assertEquals("jeremy.blachman@enron.com,a..bibi@enron.com,raymond.bowen@enron.com,london.brown@enron.com,rick.buy@enron.com,richard.causey@enron.com,wade.cline@enron.com,david.cox@enron.com,david.delainey@enron.com,james.derrick@enron.com,m..elliott@enron.com,jim.fallon@enron.com,andrew.fastow@enron.com,mark.frevert@enron.com,ben.glisan@enron.com,kevin.hannon@enron.com,rod.hayslett@enron.com,stanley.horton@enron.com,a..hughes@enron.com,steven.kean@enron.com,louise.kitchen@enron.com,mark.koenig@enron.com,kenneth.lay@enron.com,john.lavorato@enron.com,dan.leff@enron.com,danny.mccarty@enron.com,mike.mcconnell@enron.com,rebecca.mcdonald@enron.com,jeffrey.mcmahon@enron.com,mark.metts@enron.com,mark.muller@enron.com,cindy.olson@enron.com,lou.pai@enron.com,kenneth.rice@enron.com,matthew.scrimshaw@enron.com,a..shankman@enron.com,jeffrey.sherrick@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,marty.sunde@enron.com,greg.whalley@enron.com,greg.piper@enron.com,janet.dietrich@enron.com",m.getToAsString());
        assertNull(m.getSubject());
        assertEquals("g.g..garcia@enron.com,k..heathman@enron.com,sharron.westbrook@enron.com,kay.chapman@enron.com,j.harris@enron.com,bridget.maronge@enron.com,nicki.daw@enron.com,inez.dauterive@enron.com,ann.brown@enron.com,cindy.stark@enron.com,maureen.mcvicker@enron.com,joannie.williamson@enron.com,rosalee.fleming@enron.com,l..wells@enron.com,cathy.phillips@enron.com,loretta.brelsford@enron.com,sue.ford@enron.com,dolores.fisher@enron.com,karen.owens@enron.com,dorothy.dalton@enron.com,mercedes.estrada@enron.com,christina.grow@enron.com,lauren.urquhart@enron.com,sherri.sera@enron.com,liz.taylor@enron.com,kathy.mcmahon@enron.com,suzanne.danz@enron.com,peggy.mccurley@enron.com,marsha.schiller@enron.com,marisa.rapacioli@enron.com,l..paxton@enron.com,connie.blackwood@enron.com,tammie.schoppe@enron.com,kimberly.hillis@enron.com,jennifer.burns@enron.com,sharon.dick@enron.com,kathy.dodgen@enron.com,kerry.ferrari@enron.com,carol.moffett@enron.com,jennifer.adams@enron.com,leah.rijo@enron.com,lucy.marshall@enron.com,kathy.campos@enron.com,julie.armstrong@enron.com,binky.davidson@enron.com,mrudula.gadade@enron.com,kelly.johnson@enron.com,rebecca.carter@enron.com,tina.spiller@enron.com,vivianna.bolen@enron.com,linda.hawkins@enron.com,vanessa.bob@enron.com,esmeralda.hinojosa@enron.com",m.getCcAsString());
        assertEquals("g.g..garcia@enron.com,k..heathman@enron.com,sharron.westbrook@enron.com,kay.chapman@enron.com,j.harris@enron.com,bridget.maronge@enron.com,nicki.daw@enron.com,inez.dauterive@enron.com,ann.brown@enron.com,cindy.stark@enron.com,maureen.mcvicker@enron.com,joannie.williamson@enron.com,rosalee.fleming@enron.com,l..wells@enron.com,cathy.phillips@enron.com,loretta.brelsford@enron.com,sue.ford@enron.com,dolores.fisher@enron.com,karen.owens@enron.com,dorothy.dalton@enron.com,mercedes.estrada@enron.com,christina.grow@enron.com,lauren.urquhart@enron.com,sherri.sera@enron.com,liz.taylor@enron.com,kathy.mcmahon@enron.com,suzanne.danz@enron.com,peggy.mccurley@enron.com,marsha.schiller@enron.com,marisa.rapacioli@enron.com,l..paxton@enron.com,connie.blackwood@enron.com,tammie.schoppe@enron.com,kimberly.hillis@enron.com,jennifer.burns@enron.com,sharon.dick@enron.com,kathy.dodgen@enron.com,kerry.ferrari@enron.com,carol.moffett@enron.com,jennifer.adams@enron.com,leah.rijo@enron.com,lucy.marshall@enron.com,kathy.campos@enron.com,julie.armstrong@enron.com,binky.davidson@enron.com,mrudula.gadade@enron.com,kelly.johnson@enron.com,rebecca.carter@enron.com,tina.spiller@enron.com,vivianna.bolen@enron.com,linda.hawkins@enron.com,vanessa.bob@enron.com,esmeralda.hinojosa@enron.com",m.getBccAsString());
//        assertEquals("1.0", m.getHeaders().get("Mime-Version").iterator().next());
//        assertEquals("text/plain; charset=us-ascii", m.getHeaders().get("Content-Type").iterator().next());
//        assertEquals("7bit", m.getHeaders().get("Content-Transfer-Encoding").iterator().next());
//        assertEquals("Sherri Sera", m.getHeaders().get("X-From").iterator().next());
//        assertEquals("\"Stephen L Comeau\" <slc1856@sunflower.com>", m.getHeaders().get("X-To").iterator().next());
//        assertEquals("\\Jeffrey_Skilling_Dec2000\\Notes Folders\\Sent", m.getHeaders().get("X-Folder").iterator().next());
//        assertEquals("SKILLING-J", m.getHeaders().get("X-Origin").iterator().next());
//        assertEquals("jskillin.nsf", m.getHeaders().get("X-FileName").iterator().next());
        
//        assertEquals(11, m.getHeaders().size());
        assertEquals(86, m.getBodyTokens().size());
        assertEquals(12249, m.getRawBytes().length);
    }
    
    @Test
    public void test4() throws Exception {
        String path = DATA_FILE_4;
        Message m = parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
        assertNotNull(m.getId());
        assertTrue(m.getTimestamp() > 0);
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("inbox", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("a.shreck@occmail.occ.state.ok.us", m.getFrom());
        assertEquals("jeff.skilling@enron.com", m.getToAsString());
        assertEquals("Fall Forum", m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
//        assertEquals("1.0", m.getHeaders().get("Mime-Version").iterator().next());
//        assertEquals("text/plain; charset=us-ascii", m.getHeaders().get("Content-Type").iterator().next());
//        assertEquals("7bit", m.getHeaders().get("Content-Transfer-Encoding").iterator().next());
//        assertEquals("Sherri Sera", m.getHeaders().get("X-From").iterator().next());
//        assertEquals("\"Stephen L Comeau\" <slc1856@sunflower.com>", m.getHeaders().get("X-To").iterator().next());
//        assertEquals("\\Jeffrey_Skilling_Dec2000\\Notes Folders\\Sent", m.getHeaders().get("X-Folder").iterator().next());
//        assertEquals("SKILLING-J", m.getHeaders().get("X-Origin").iterator().next());
//        assertEquals("jskillin.nsf", m.getHeaders().get("X-FileName").iterator().next());
        
//        assertEquals(11, m.getHeaders().size());
        assertEquals(37, m.getBodyTokens().size());
        assertEquals(886, m.getRawBytes().length);
    }
    
    @Test
    public void test5() throws Exception {
        String path = DATA_FILE_5;
        Message m = parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
        assertNotNull(m.getId());
        assertTrue(m.getTimestamp() > 0);
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("sent", m.getFolder());
        assertEquals("83.", m.getFileName());
        
        assertEquals("sherri.sera@enron.com", m.getFrom());
        assertEquals("slc1856@sunflower.com", m.getToAsString());
        assertEquals("Re: Thanks for dinner", m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
//        assertEquals("1.0", m.getHeaders().get("Mime-Version").iterator().next());
//        assertEquals("text/plain; charset=us-ascii", m.getHeaders().get("Content-Type").iterator().next());
//        assertEquals("7bit", m.getHeaders().get("Content-Transfer-Encoding").iterator().next());
//        assertEquals("Sherri Sera", m.getHeaders().get("X-From").iterator().next());
//        assertEquals("\"Stephen L Comeau\" <slc1856@sunflower.com>", m.getHeaders().get("X-To").iterator().next());
//        assertEquals("\\Jeffrey_Skilling_Dec2000\\Notes Folders\\Sent", m.getHeaders().get("X-Folder").iterator().next());
//        assertEquals("SKILLING-J", m.getHeaders().get("X-Origin").iterator().next());
//        assertEquals("jskillin.nsf", m.getHeaders().get("X-FileName").iterator().next());
        
//        assertEquals(11, m.getHeaders().size());
        assertEquals(21, m.getBodyTokens().size());
        assertEquals(605, m.getRawBytes().length);
    }
}
