package org.apache.mahout.fpm.bigfim;

import static org.apache.mahout.fpm.AllTests.setField;
import static org.easymock.EasyMock.createMock;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.fpm.util.Trie;
import org.easymock.EasyMock;
import org.junit.Test;

public class AprioriPhaseMapperTest {
  
  private static Set<Integer> create_Set_1() {
    Set<Integer> set = new HashSet<Integer>();
    set.add(1);
    set.add(2);
    set.add(3);
    set.add(4);
    set.add(5);
    return set;
  }
  
  private static Trie create_Count_Trie_Empty() {
    return new Trie(-1);
  }
  
  private static Trie create_Count_Trie_1() {
    Trie trie = new Trie(-1);
    
    Trie child1 = trie.getChild(1);
    child1.getChild(2);
    child1.getChild(3);
    child1.getChild(4);
    
    Trie child2 = trie.getChild(2);
    child2.getChild(3);
    child2.getChild(5);
    
    Trie child3 = trie.getChild(3);
    child3.getChild(4);
    
    Trie child4 = trie.getChild(4);
    child4.getChild(5);
    
    return trie;
  }
  
  private static String[] data = new String[] {"1 2 3 4", "2 3 4", "1 3 5", "1", "3 4 5", "1 3 4 5", "2 5", "1 3 4"};
  
  @Test
  public void phase_1_No_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "phase", 1);
    setField(mapper, "countTrie", create_Count_Trie_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_1_With_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text("1"), new IntWritable(5));
    ctx.write(new Text("2"), new IntWritable(3));
    ctx.write(new Text("3"), new IntWritable(6));
    ctx.write(new Text("4"), new IntWritable(5));
    ctx.write(new Text("5"), new IntWritable(4));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "phase", 1);
    setField(mapper, "countTrie", create_Count_Trie_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text("1 2"), new IntWritable(0));
    ctx.write(new Text("1 3"), new IntWritable(0));
    ctx.write(new Text("1 4"), new IntWritable(0));
    ctx.write(new Text("2 3"), new IntWritable(0));
    ctx.write(new Text("2 5"), new IntWritable(0));
    ctx.write(new Text("3 4"), new IntWritable(0));
    ctx.write(new Text("4 5"), new IntWritable(0));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", 2);
    setField(mapper, "countTrie", create_Count_Trie_1());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_No_Input_Empty_Count_Trie() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", 2);
    setField(mapper, "countTrie", create_Count_Trie_Empty());
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    ctx.write(new Text("1 2"), new IntWritable(1));
    ctx.write(new Text("1 3"), new IntWritable(4));
    ctx.write(new Text("1 4"), new IntWritable(3));
    ctx.write(new Text("2 3"), new IntWritable(2));
    ctx.write(new Text("2 5"), new IntWritable(1));
    ctx.write(new Text("3 4"), new IntWritable(5));
    ctx.write(new Text("4 5"), new IntWritable(2));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", 2);
    setField(mapper, "countTrie", create_Count_Trie_1());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void phase_2_With_Input_Empty_Count_Trie() throws Exception {
    AprioriPhaseMapper.Context ctx = createMock(Mapper.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseMapper mapper = new AprioriPhaseMapper();
    setField(mapper, "singletons", create_Set_1());
    setField(mapper, "phase", 2);
    setField(mapper, "countTrie", create_Count_Trie_Empty());
    
    for (int i = 0; i < data.length; i++) {
      mapper.map(new LongWritable(i), new Text(data[i]), ctx);
    }
    
    mapper.cleanup(ctx);
    
    EasyMock.verify(ctx);
  }
}
